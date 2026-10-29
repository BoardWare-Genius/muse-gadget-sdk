import asyncio
import copy
import json
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from notifications import MAX_ITEMS, NotificationInbox


class SyncError(Exception):
    pass


class Nano:
    base, token = "http://test.invalid", "private-test-token"

    def __init__(self):
        self.feed, self.events, self.calls = [], [], []
        self.threads = [{"id": "background", "title": "测试任务", "busy": False}]
        self.fail = False

    async def request(self, method, path, **kwargs):
        self.calls.append((method, path))
        if self.fail:
            raise SyncError("offline")
        assert method == "GET"  # receiving/reading never approves or answers a task
        if path == "/api/feed":
            assert kwargs["response_type"] is list
            return copy.deepcopy(self.feed)
        return {"threads": copy.deepcopy(self.threads)} if path == "/api/threads" else {"events": self.events}


class Socket:
    closed = False

    def __init__(self):
        self.messages = []

    async def send_json(self, payload):
        self.messages.append(copy.deepcopy(payload))


class Session:
    def __init__(self):
        self.ws = Socket()
        self.thread = self.task = None
        self.awaiting_answer = False
        self.notification_version = -1
        self.notification_tokens = set()


class NotificationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "private" / "notifications.json"
        self.nano = Nano()
        self.inbox = self.restore()
        self.session = Session()
        self.inbox.subscribe(self.session)

    def restore(self, **extra):
        return NotificationInbox(self.nano, {"state_file": str(self.path), **extra}, SyncError)

    async def asyncTearDown(self):
        self.temp.cleanup()

    def result(self, number=1, kind="background", text="后台结果已完成"):
        event = {"id": f"event-{number}", "kind": kind, "thread": "background",
                 "thread_title": "测试任务", "title": "定时任务", "text": text}
        self.nano.feed.insert(0, event)
        self.nano.events.append({"id": event["id"], "source": "background", "type": "assistant", "final": True})
        return event

    async def test_starts_from_now_then_waits_for_final_task_result(self):
        self.result(0)
        await self.inbox.poll()
        self.assertEqual(self.inbox.items, [])
        self.result(1)
        self.nano.threads[0]["busy"] = True
        await self.inbox.poll()
        self.assertEqual(self.inbox.items, [])
        self.nano.threads[0]["busy"] = False
        self.nano.events[-1]["final"] = False
        await self.inbox.poll()
        self.assertEqual(self.inbox.items, [])
        self.nano.events[-1]["final"] = True
        await self.inbox.poll()
        self.assertEqual(len(self.inbox.items), 1)
        await self.inbox.poll()
        self.assertEqual(len(self.inbox.items), 1)

    async def test_receipt_requires_delivery_is_durable_and_updates_other_devices(self):
        await self.inbox.poll()
        self.result()
        await self.inbox.poll()
        token = self.inbox.items[0]["token"]
        await self.inbox.read(self.session, "f" * 32)
        await self.inbox.read(Session(), token)
        self.assertEqual(len(self.inbox.items), 1)
        other = Session()
        self.inbox.subscribe(other)
        await self.inbox.publish()
        await self.inbox.read(self.session, token)
        self.assertEqual(other.ws.messages[-1]["items"], [])
        await self.inbox.read(self.session, token)
        restored = self.restore()
        await restored.poll()
        self.assertEqual(restored.items, [])
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.path.parent.stat().st_mode), 0o700)
        self.assertNotIn(self.nano.token, self.path.read_text())

    async def test_unread_survives_restart_and_upstream_outage(self):
        await self.inbox.poll()
        self.result()
        await self.inbox.poll()
        restored = self.restore()
        self.assertEqual(restored.snapshot(), self.inbox.snapshot())
        session = Session()
        restored.subscribe(session)
        self.nano.fail = True
        with self.assertRaises(SyncError):
            await restored.poll()
        await restored.publish()
        self.assertEqual(len(session.ws.messages[-1]["items"]), 1)

    async def test_pending_actions_but_not_current_chat_or_quiet_results(self):
        self.result(1, "approval")
        self.result(2, "question")
        self.result(3)["quiet"] = True
        await self.inbox.poll()
        self.assertEqual({i["kind"] for i in self.inbox.items}, {"approval", "question"})
        self.assertTrue(all("请到网页" in i["title"] for i in self.inbox.items))
        self.nano.feed = []  # handled on the website
        await self.inbox.poll()
        self.assertEqual(self.inbox.items, [])
        self.session.thread, self.session.awaiting_answer = "background", True
        self.result(4, "question")
        await self.inbox.poll()
        self.assertEqual(self.inbox.items, [])
        self.session.awaiting_answer = False
        await self.inbox.poll()
        self.assertEqual(len(self.inbox.items), 1)

    async def test_failures_files_and_wire_size_are_bounded(self):
        await self.inbox.poll()
        self.result(1)
        self.nano.events[-1].update(type="notice", level="error")
        await self.inbox.poll()
        self.assertEqual(self.inbox.items[0]["kind"], "error")
        self.result(2, "artifact")["path"] = "notes/result.txt"
        await self.inbox.poll()
        self.assertIn("notes/result.txt", self.inbox.items[0]["text"])
        for n in range(3, 20):
            event = self.result(n, text='\x01\\"\n中文' * 1000)
            event.update(title='\\"\n' * 1000, thread_title='\\"\n' * 1000)
        await self.inbox.poll()
        self.assertEqual(len(self.inbox.items), MAX_ITEMS)
        self.assertLessEqual(len(json.dumps(self.inbox.snapshot(), ensure_ascii=False).encode()), 8192)
        self.assertTrue(all(len(i["text"].encode()) <= 480 and len(i["title"].encode()) <= 128 for i in self.inbox.items))

    async def test_account_change_cannot_leak_cached_messages(self):
        await self.inbox.poll()
        self.result()
        await self.inbox.poll()
        self.nano.token = "another-account"
        restored = self.restore()
        self.assertFalse(restored.initialized)
        self.assertEqual(restored.items, [])

    async def test_storage_failure_does_not_ack_or_lose_notifications(self):
        await self.inbox.poll()
        self.result()
        with patch.object(self.inbox, "save", side_effect=OSError("disk unavailable")):
            with self.assertRaises(OSError):
                await self.inbox.poll()
        self.assertEqual(self.inbox.items, [])
        await self.inbox.poll()
        with patch.object(self.inbox, "save", side_effect=OSError("disk unavailable")):
            with self.assertRaises(OSError):
                await self.inbox.read(self.session, self.inbox.items[0]["token"])
        self.assertEqual(len(self.inbox.items), 1)

    async def test_disabled_or_closed_sessions_receive_nothing(self):
        disabled = self.restore(enabled=False)
        session = Session()
        disabled.subscribe(session)
        await disabled.publish()
        self.assertEqual(session.ws.messages, [])
        self.session.ws.closed = True
        await self.inbox.publish()
        self.assertNotIn(self.session, self.inbox.sessions)

    async def test_receipt_during_upstream_fetch_cannot_resurrect_read_item(self):
        await self.inbox.poll()
        self.result()
        await self.inbox.poll()
        started, release = asyncio.Event(), asyncio.Event()
        original = self.nano.request

        async def delayed(method, path, **kwargs):
            if path == "/api/feed":
                started.set()
                await release.wait()
            return await original(method, path, **kwargs)

        with patch.object(self.nano, "request", side_effect=delayed):
            poll = asyncio.create_task(self.inbox.poll())
            await started.wait()
            await self.inbox.read(self.session, self.inbox.items[0]["token"])
            release.set()
            await poll
        self.assertEqual(self.inbox.items, [])
        self.assertEqual(self.restore().items, [])

    async def test_corrupt_cache_does_not_prevent_chat_server_startup(self):
        self.path.parent.mkdir()
        self.path.write_text('{"initialized": true, "namespace":')
        with self.assertLogs("gateway.notifications", level="WARNING"):
            restored = self.restore()
        self.assertEqual(restored.items, [])
        self.assertFalse(restored.initialized)
        self.path.write_text(json.dumps({"namespace": self.inbox.namespace, "seen": "invalid", "items": []}))
        with self.assertLogs("gateway.notifications", level="WARNING"):
            self.assertFalse(self.restore().initialized)

    async def test_thread_endpoint_accepts_list_response_used_by_live_server(self):
        original = self.nano.request

        async def request(method, path, **kwargs):
            if path == "/api/threads":
                self.assertEqual(kwargs["response_type"], (list, dict))
                return self.nano.threads
            return await original(method, path, **kwargs)

        with patch.object(self.nano, "request", side_effect=request):
            await self.inbox.poll()
            self.result()
            await self.inbox.poll()
        self.assertEqual(len(self.inbox.items), 1)
