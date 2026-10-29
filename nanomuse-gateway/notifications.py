"""Bounded, durable notification inbox for devices sharing the gateway key.

Read receipts are local to this gateway. They never approve a tool, answer a
question, or change nanoMuse's tasks. Only explicit subscribers receive data.
"""
import asyncio
import hashlib
import json
import logging
import os
from pathlib import Path
import secrets
from urllib.parse import quote

LOG = logging.getLogger("gateway.notifications")
MAX_ITEMS = 6
MAX_SEEN = 2048


def bounded(value, limit):
    text = value if isinstance(value, str) else ""
    text = "".join(c for c in text if ord(c) >= 32 or c in "\n\t")
    raw = text.encode("utf-8", "ignore")
    if len(raw) <= limit:
        return raw.decode("utf-8")
    suffix = "…（电脑查看全文）"
    return raw[:limit - len(suffix.encode())].decode("utf-8", "ignore") + suffix


class NotificationInbox:
    def __init__(self, nano, config, upstream_error):
        self.nano = nano
        self.upstream_error = upstream_error
        self.enabled = config.get("enabled", True) is True
        self.interval = max(1, min(float(config.get("poll_interval", 5)), 60))
        self.path = Path(config.get("state_file", ".state/notifications.json"))
        self.namespace = hashlib.sha256((nano.base + "\0" + nano.token).encode()).hexdigest()
        self.seen = []
        self.items = []
        self.initialized = False
        self.version = 1
        self.sessions = set()
        try:
            self._load()
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            self.seen, self.items, self.initialized = [], [], False
            LOG.warning("Notification cache unreadable; starting a fresh inbox")

    def _load(self):
        if not self.path.exists():
            return
        data = json.loads(self.path.read_text("utf-8"))
        if data.get("namespace") != self.namespace:
            return
        if not isinstance(data.get("seen"), list) or not isinstance(data.get("items"), list):
            raise ValueError("invalid notification cache lists")
        self.initialized = data.get("initialized") is True
        self.seen = data["seen"][-MAX_SEEN:]
        self.items = data["items"][:MAX_ITEMS]
        if (any(not isinstance(key, str) for key in self.seen)
                or any(not isinstance(item, dict) or any(not isinstance(item.get(field), str)
                       or len(item[field].encode("utf-8")) > limit
                       for field, limit in (("token", 32), ("key", 64), ("kind", 16),
                                            ("title", 128), ("text", 480)))
                       or len(item["token"]) != 32
                       or any(c not in "0123456789abcdef" for c in item["token"])
                       for item in self.items)):
            raise ValueError("invalid notification state")

    def save(self):
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        temporary = self.path.with_name(self.path.name + "." + secrets.token_hex(8) + ".tmp")
        data = {"namespace": self.namespace, "initialized": self.initialized,
                "seen": self.seen[-MAX_SEEN:], "items": self.items[:MAX_ITEMS]}
        try:
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(data, stream, ensure_ascii=False)
            os.replace(temporary, self.path)
        finally:
            temporary.unlink(missing_ok=True)

    def subscribe(self, session):
        if self.enabled:
            self.sessions.add(session)
            session.notification_version = -1

    def snapshot(self):
        return {"type": "notifications", "items": [
            {key: item[key] for key in ("token", "kind", "title", "text")}
            for item in self.items]}

    async def publish(self):
        for session in tuple(self.sessions):
            if session.ws.closed:
                self.sessions.discard(session)
                continue
            if session.notification_version != self.version:
                version, snapshot = self.version, self.snapshot()
                try:
                    await session.ws.send_json(snapshot)
                    session.notification_version = version
                    session.notification_tokens = {item["token"] for item in snapshot["items"]}
                except (ConnectionError, RuntimeError):
                    self.sessions.discard(session)

    async def read(self, session, token):
        if (session not in self.sessions or not isinstance(token, str)
                or token not in session.notification_tokens):
            return
        previous = self.items
        self.items = [item for item in previous if item["token"] != token]
        if self.items != previous:
            try:
                self.save()
            except OSError:
                self.items = previous
                raise
            self.version += 1
        # A retried receipt also gets a fresh snapshot, even if already read.
        session.notification_version = -1
        await self.publish()

    async def poll(self):
        feed, data = await asyncio.gather(
            self.nano.request("GET", "/api/feed", response_type=list, params={"limit": "60"}),
            self.nano.request("GET", "/api/threads", response_type=(list, dict)))
        threads = data.get("threads") if isinstance(data, dict) else data
        if (not isinstance(threads, list) or any(not isinstance(t, dict) for t in threads)
                or any(not isinstance(item, dict) for item in feed)):
            raise self.upstream_error("nanoMuse 通知列表格式无效")
        metadata = {t.get("id"): t for t in threads if isinstance(t.get("id"), str)}
        active = {s.thread for s in self.sessions if s.thread and
                  (s.awaiting_answer or s.task is not None and not s.task.done())}
        pending_keys = set()
        candidates = []
        timelines = {}
        # Feed is newest first. Insert oldest first so the inbox stays newest first.
        for event in reversed(feed):
            kind, thread, event_id = (event.get(k) for k in ("kind", "thread", "id"))
            if (kind not in ("background", "artifact", "approval", "question")
                    or not isinstance(thread, str) or not 1 <= len(thread) <= 256
                    or not isinstance(event_id, str) or not 1 <= len(event_id) <= 256
                    or event.get("quiet") or thread not in metadata):
                continue
            key = hashlib.sha256(json.dumps([thread, kind, event_id]).encode()).hexdigest()
            actionable = kind in ("approval", "question")
            if actionable:
                pending_keys.add(key)
                # The current chat already presents its own approvals/questions.
                if thread in active:
                    continue
            elif metadata[thread].get("busy") or metadata[thread].get("queued", 0):
                continue
            if key in self.seen:
                continue
            if not actionable and not self.initialized:
                candidates.append((key, None))  # first install: no history flood
                continue
            if kind == "background":
                if thread not in timelines:
                    response = await self.nano.request("GET", f"/api/threads/{quote(thread, safe='')}/events",
                                                       params={"limit": "1000"})
                    timelines[thread] = response.get("events", [])
                original = next((e for e in timelines[thread] if isinstance(e, dict)
                                 and e.get("id") == event_id), {})
                if original.get("source") != "background":
                    continue
                if original.get("type") == "notice" and original.get("level") == "error":
                    kind = "error"
                elif original.get("type") != "assistant" or original.get("final") is not True:
                    continue  # narration is not a completed task result
            titles = {"background": "任务更新", "artifact": "新文件", "error": "任务失败",
                      "approval": "待批准（请到网页）", "question": "待补充信息（请到网页）"}
            title = titles[kind] + "：" + bounded(event.get("thread_title") or event.get("title"), 64)
            detail = bounded(event.get("title"), 80) + "\n" + bounded(event.get("text"), 384)
            if kind == "artifact" and event.get("path"):
                detail += "\n" + bounded(event["path"], 96)
            candidates.append((key, {"key": key, "token": secrets.token_hex(16), "kind": kind,
                                      "title": bounded(title, 128), "text": bounded(detail, 480)}))
        # No network awaits while changing state: a receipt received during the
        # upstream fetch must not be undone by a stale copy of the inbox.
        old_seen, old_items, old_initialized = self.seen[:], self.items[:], self.initialized
        changed = False
        for key, item in candidates:
            if key in self.seen:
                continue
            self.seen.append(key)
            if item is not None:
                self.items.insert(0, item)
                self.items = self.items[:MAX_ITEMS]
            changed = True
        retained = [item for item in self.items if item["kind"] not in ("approval", "question")
                    or item["key"] in pending_keys]
        changed |= retained != self.items
        self.items = retained
        changed |= not self.initialized
        self.initialized = True
        self.seen = self.seen[-MAX_SEEN:]
        if changed:
            try:
                self.save()
            except OSError:
                self.seen, self.items, self.initialized = old_seen, old_items, old_initialized
                raise
            self.version += 1
        await self.publish()

    async def run(self):
        while True:
            if self.enabled and self.sessions:
                try:
                    await self.poll()
                except (self.upstream_error, OSError, ValueError, TypeError, KeyError):
                    LOG.warning("Notification sync unavailable; will retry")
                # Cached unread messages remain available during an upstream outage.
                await self.publish()
            await asyncio.sleep(self.interval)
