"""CoreS3 gateway: bounded PCM uploads and text turns to a local nanoMuse.

Run: python gateway.py --config config.toml
Only the gateway's limited device key goes on the board. nanoMuse's server
credential stays here. ASR is intentionally optional; no fake transcription.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import hmac
import io
import json
import logging
import os
import re
import secrets
from pathlib import Path
import shutil
import struct
import time
import tomllib
from urllib.parse import quote
import wave

from aiohttp import ClientError, ClientSession, ClientTimeout, FormData, WSMsgType, web
from notifications import NotificationInbox
from images import IMAGE_MAGIC, ImageBridge, display_size, image_paths

LOG = logging.getLogger("gateway.latency")

RATE = 16000
MAX_AUDIO = RATE * 2 * 16
MAX_TEXT = 4000
MAX_REPLY = 6000
MAX_JSON = 8192
UPLOAD_TIMEOUT = 30
MAX_SPEECH = RATE * 2 * 60
MAX_TTS_WAV = 24_000_000
MAX_APPROVAL_TEXT = 4096
PROGRESS_INTERVAL = 2.0


class ReplyProgress:
    """Only new model/tool activity renews the idle deadline; polling cannot."""
    def __init__(self, idle_timeout, max_timeout):
        now = time.monotonic()
        self.idle_timeout = idle_timeout
        self.idle_deadline = now + idle_timeout
        self.deadline = now + max_timeout
        self.last_report = now
        self.dirty = False
        self.fingerprint = None

    def touch(self):
        self.idle_deadline = time.monotonic() + self.idle_timeout
        self.dirty = True

    def observe(self, events):
        # Timestamps/status heartbeats alone do not count as progress. Keep
        # content private: only a digest, never model/tool text in logs.
        fields = ("id", "type", "text", "reasoning", "final", "status", "output", "path", "action")
        meaningful = [{k: e[k] for k in fields if k in e} for e in events
                      if e.get("type") in ("assistant", "tool", "approval", "question", "artifact", "error")]
        fingerprint = hashlib.sha256(json.dumps(meaningful, sort_keys=True).encode()).digest()
        if meaningful and fingerprint != self.fingerprint:
            self.touch()
        self.fingerprint = fingerprint

    async def report(self, send):
        now = time.monotonic()
        if self.dirty and now - self.last_report >= PROGRESS_INTERVAL:
            await send("progress")
            self.last_report = now
            self.dirty = False

    def expired(self):
        now = time.monotonic()
        return now >= self.deadline or now >= self.idle_deadline


class TurnError(Exception):
    """An intentionally public error, safe to send to the board."""


class UpstreamHTTPError(TurnError):
    def __init__(self, status: int):
        self.status = status
        super().__init__(f"nanoMuse 请求失败 (HTTP {status})")


def approval_details(event: dict) -> tuple[str, bool]:
    """Use the server's approval preview, with no silent truncation of consent."""
    parts = ["仅允许本次操作", f"工具：{event.get('tool', '')}",
             f"风险：{event.get('risk', '')}", str(event.get("summary", ""))]
    for key, label in (("purpose", "目的"), ("target", "目标"), ("egress_target", "网络目标")):
        if event.get(key):
            parts.append(f"{label}：{event[key]}")
    for key, label in (("reasons", "原因"), ("warnings", "警告")):
        if isinstance(event.get(key), list):
            parts.extend(f"{label}：{value}" for value in event[key])
    if event.get("args"):
        parts.append("参数预览：\n" + json.dumps(event["args"], ensure_ascii=False, indent=2))
    text = "\n".join(parts)
    options = event.get("grant_options", ["once"])
    allowed = isinstance(options, list) and "once" in options
    if (len(text.encode()) > MAX_APPROVAL_TEXT
            or len(json.dumps({"text": text}, ensure_ascii=False).encode()) > MAX_JSON - 512):
        return "操作详情过长，请在 nanoMuse 网页查看完整内容后处理。\n此处只能拒绝。", False
    if not allowed:
        text = "此操作不能单次授权，请在 nanoMuse 网页处理。\n" + text
    if len(text.encode()) > MAX_APPROVAL_TEXT:
        return "请在 nanoMuse 网页查看完整操作详情。此处只能拒绝。", False
    return text, allowed


async def read_object(response, limit: int, response_type=dict):
    raw = bytearray()
    async for chunk in response.content.iter_chunked(16384):
        raw.extend(chunk)
        if len(raw) > limit:
            raise TurnError("服务返回内容过大")
    value = json.loads(raw)
    if not isinstance(value, response_type):
        raise TurnError("服务返回了无效的 JSON 对象")
    return value


def trim_utf8(text: str, limit: int = MAX_REPLY) -> str:
    raw = text.encode("utf-8")
    if len(raw) <= limit:
        return text
    suffix = "\n[回复较长，请在电脑查看全文]"
    return raw[: limit - len(suffix.encode())].decode("utf-8", "ignore") + suffix


def wav_bytes(pcm: bytes) -> bytes:
    out = io.BytesIO()
    with wave.open(out, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(RATE)
        wav.writeframes(pcm)
    return out.getvalue()


def valid_id(value: object) -> bool:
    return type(value) is int and 0 < value <= 0xFFFFFFFF


def speech_provider(config: dict) -> str:
    provider = config.get("provider", "openai")
    if provider not in ("openai", "blackbox"):
        raise ValueError("speech provider must be 'openai' or 'blackbox'")
    return provider


def transcript_text(data: dict, provider: str) -> str:
    text = data.get("text")
    if provider == "blackbox" and not isinstance(text, str):
        # SenseVoice returns a list. Prefer clean_text to omit language/emotion
        # tokens, while preserving segment order for multi-part recordings.
        results = data.get("result")
        if isinstance(results, list) and all(isinstance(item, dict) for item in results):
            parts = []
            for item in results:
                part = item.get("clean_text")
                if not isinstance(part, str) or not part.strip():
                    part = item.get("text")
                if isinstance(part, str) and part.strip():
                    parts.append(part.strip())
            text = "\n".join(parts)
    if not isinstance(text, str) or not text.strip():
        raise TurnError("没有识别到文字")
    if len(text.encode()) > MAX_TEXT:
        raise TurnError("识别文字过长")
    return text.strip()


class NanoMuse:
    """REST adapter, using a private thread for each device connection.

    Poll the persisted timeline, including updates that mark an existing
    assistant event final. Step narration is never mistaken for a final reply.
    """

    def __init__(self, http: ClientSession, config: dict):
        self.http = http
        self.base = config.get("url", "http://127.0.0.1:8787").rstrip("/")
        self.token = os.environ.get(config.get("token_env", "NANOMUSE_SERVER_TOKEN"), "")
        self.timeout = float(config.get("turn_timeout", 150))
        self.max_timeout = float(config.get("max_turn_timeout", 900))
        if not 0 < self.timeout <= 900 or not 0 < self.max_timeout <= 900:
            raise ValueError("nanomuse timeouts must be greater than 0 and at most 900 seconds")
        self.poll_interval = float(config.get("poll_interval", 0.4))

    async def request(self, method: str, path: str, response_type=dict, **kwargs):
        if not self.token:
            raise TurnError("电脑端尚未配置 NANOMUSE_SERVER_TOKEN")
        try:
            async with self.http.request(
                method, self.base + path,
                headers={"Authorization": "Bearer " + self.token},
                timeout=ClientTimeout(total=10), **kwargs,
            ) as response:
                if response.status in (401, 403):
                    raise TurnError("nanoMuse 认证失败，请检查电脑端 server token")
                if response.status >= 400:
                    raise UpstreamHTTPError(response.status)
                return await read_object(response, 2_000_000, response_type)
        except (ClientError, OSError, asyncio.TimeoutError, ValueError) as exc:
            raise TurnError("电脑无法连接 nanoMuse 本地服务") from exc

    async def create_thread(self) -> str:
        data = await self.request("POST", "/api/threads", json={"title": "CoreS3"})
        thread = data.get("id")
        if not isinstance(thread, str) or not thread:
            raise TurnError("nanoMuse 返回了无效的会话")
        return thread

    async def stop(self, thread: str) -> None:
        with contextlib.suppress(TurnError):
            await self.request("POST", f"/api/threads/{quote(thread, safe='')}/stop")

    async def reply(self, thread: str, text: str, send, on_delta=None, on_approvals=None,
                    on_question=None, on_images=None, _progress=None) -> str:
        # Subscribe before sending so even the first tokens cannot be missed.
        # Older nanoMuse servers without /ws retain the REST final-reply path.
        if _progress is None:
            progress = ReplyProgress(self.timeout, self.max_timeout)
            live = None
            try:
                live = await asyncio.wait_for(
                    self.http.ws_connect(self.base + "/ws", max_msg_size=2_000_000), 5)
                await live.send_json({"kind": "auth", "token": self.token})
                hello = await asyncio.wait_for(live.receive_json(), 5)
                if not isinstance(hello, dict) or hello.get("kind") != "hello":
                    raise ValueError("missing hello")
            except asyncio.CancelledError:
                if live is not None:
                    await live.close()
                raise
            except (ClientError, OSError, ValueError, TypeError, asyncio.TimeoutError):
                if live is not None:
                    await live.close()
                return await self.reply(thread, text, send, on_approvals=on_approvals,
                                        on_question=on_question, on_images=on_images, _progress=progress)

            async def consume():
                async for message in live:
                    if message.type != WSMsgType.TEXT:
                        continue
                    data = json.loads(message.data)
                    if isinstance(data, dict) and data.get("kind") == "delta" and data.get("thread") == thread:
                        delta = data.get("text")
                        if isinstance(delta, str) and delta:
                            progress.touch()
                            if on_delta is not None:
                                await on_delta(delta)

            reader = asyncio.create_task(consume())
            try:
                return await self.reply(thread, text, send, on_approvals=on_approvals,
                                        on_question=on_question, on_images=on_images, _progress=progress)
            finally:
                reader.cancel()
                with contextlib.suppress(asyncio.CancelledError, ValueError, ClientError):
                    await reader
                await live.close()

        path = f"/api/threads/{quote(thread, safe='')}"
        result = await self.request("POST", path + "/send", json={"text": text, "language": "zh-CN"})
        event = result.get("event")
        anchor = event.get("id") if isinstance(event, dict) else None
        if not isinstance(anchor, str) or not anchor:
            raise TurnError("nanoMuse 未返回消息 ID")
        await send("sent")
        progress = _progress
        last_poll, waiting, approval_budget = time.monotonic(), False, 120.0
        while not progress.expired():
            result = await self.request("GET", path + "/events", params={"limit": "1000"})
            events = result.get("events")
            if not isinstance(events, list) or any(not isinstance(event, dict) for event in events):
                raise TurnError("nanoMuse 返回了无效的消息列表")
            start = next((n for n, event in enumerate(events) if event.get("id") == anchor), None)
            if start is None:
                raise TurnError("nanoMuse 会话已改变，请重新发送")
            now = time.monotonic()
            if waiting:
                extension = min(now - last_poll, approval_budget)
                progress.idle_deadline += extension
                approval_budget -= extension
            last_poll = now
            foreground = [event for event in events[start + 1:]
                          if event.get("thread", thread) == thread and event.get("source") != "background"]
            progress.observe(foreground)
            await progress.report(send)
            if on_approvals is not None:
                waiting = await on_approvals([
                    event for event in events[start + 1:]
                    if event.get("thread", thread) == thread
                    and event.get("type") == "approval" and event.get("status") == "pending"
                    and event.get("source") != "background"
                ])
            for event in events[start + 1 :]:
                if event.get("thread", thread) != thread or event.get("source") == "background":
                    continue
                # ask_user suspends the upstream run rather than creating a final
                # assistant bubble. Deliver the question as this device reply;
                # its next utterance answers the pending question in the same thread.
                if (event.get("type") == "question" and event.get("status") == "pending"
                        and not waiting):
                    question = event.get("text")
                    if isinstance(question, str) and question.strip():
                        if on_question is not None:
                            await on_question()
                        return question
                if (event.get("type") == "assistant" and event.get("final") is True
                        and not event.get("quiet") and event.get("source") != "background"):
                    answer = event.get("text")
                    if isinstance(answer, str) and answer.strip():
                        if on_images is not None:
                            on_images(image_paths([
                                e for e in events[start + 1:]
                                if e.get("thread", thread) == thread and e.get("source") != "background"
                            ], answer))
                        return answer
                if event.get("type") == "error":
                    raise TurnError("nanoMuse 处理失败，请在电脑查看详情")
            await asyncio.sleep(self.poll_interval)
        reason = "model_deadline" if time.monotonic() >= progress.deadline else "model_idle_timeout"
        LOG.warning("reason=%s", reason)
        raise TurnError("nanoMuse 回复超时（达到整轮最长时限）" if reason == "model_deadline"
                        else "nanoMuse 回复超时（持续无新文字或任务进展）；请稍后重试或在网页查看")


class Session:
    def __init__(self, ws: web.WebSocketResponse, app: web.Application):
        self.ws, self.config = ws, app[CONFIG]
        self.nano = app[NANO]
        self.app_images = app[IMAGES]
        self.inbox = app[INBOX]
        self.http = app[HTTP]
        self.id: int | None = None
        self.pcm: bytearray | None = None
        self.upload_deadline = 0.0
        self.task: asyncio.Task | None = None
        self.thread: str | None = None
        self.used_ids: set[int] = set()
        self.want_audio = False
        self.streaming_audio = False
        self.approval_capable = False
        self.pending_approval: dict | None = None
        self.handled_approvals: set[str] = set()
        self.awaiting_answer = False
        self.notification_version = -1
        self.notification_tokens: set[str] = set()
        self.image_size = None
        self.image_paths: list[str] = []
        self.reply_complete = False
        self.progress_seq = 0

    def collect_images(self, paths):
        self.image_paths = paths

    async def show_image(self, turn_id):
        if not self.image_size or not self.image_paths or not self.app_images.enabled:
            return
        # One preview per reply: the latest image artifact/link. No extra model calls.
        try:
            pixels = await self.app_images.fetch(self.image_paths[-1], self.image_size)
        except (ClientError, OSError, ValueError, asyncio.TimeoutError):
            LOG.warning("turn=%s image preview unavailable", turn_id)
            await self.send("warning", "图片预览失败，文字和语音仍可使用；请在网页查看图片", turn_id)
            return
        w, h = self.image_size
        await self.ws.send_json({"type": "image", "id": turn_id, "width": w, "height": h,
                                 "format": "rgb565be", "bytes": len(pixels)})
        for offset in range(0, len(pixels), 2048):
            await self.ws.send_bytes(IMAGE_MAGIC + struct.pack("<II", turn_id, offset)
                                     + pixels[offset:offset + 2048])
        await self.send("image_end", turn_id=turn_id)

    async def question_ready(self):
        self.awaiting_answer = True
        await self.clear_approval()

    async def approval_card(self, pending: dict, error: str = ""):
        await self.ws.send_json({"type": "approval", "id": self.id, "token": pending["token"],
                                 "text": pending["text"], "can_approve": pending["can_approve"],
                                 "error": error})

    async def clear_approval(self, text: str = ""):
        pending, self.pending_approval = self.pending_approval, None
        if pending is not None and not self.ws.closed:
            await self.ws.send_json({"type": "approval_closed", "id": self.id,
                                     "token": pending["token"], "text": text})

    async def sync_approvals(self, events: list[dict]) -> bool:
        if not self.approval_capable:
            return False
        candidates = {event["id"]: event for event in events
                      if isinstance(event.get("id"), str) and 0 < len(event["id"]) <= 256
                      and event["id"] not in self.handled_approvals}
        pending = self.pending_approval
        if pending is not None and pending["event_id"] not in candidates:
            await self.clear_approval("此请求已处理或已失效")
        if self.pending_approval is None and candidates:
            event_id, event = next(iter(candidates.items()))
            text, allowed = approval_details(event)
            self.pending_approval = {"event_id": event_id, "token": secrets.token_hex(16),
                                     "text": text, "can_approve": allowed, "deciding": False}
            await self.approval_card(self.pending_approval)
        return self.pending_approval is not None

    async def decide_approval(self, data: dict):
        pending = self.pending_approval
        approved = data.get("approved")
        if (pending is None or self.task is None or self.task.done() or pending["deciding"]
                or data.get("id") != self.id or data.get("token") != pending["token"]
                or type(approved) is not bool):
            return  # No authority over another turn, device, or undisplayed card.
        if approved and not pending["can_approve"]:
            await self.approval_card(pending, "请在网页查看完整详情后处理")
            return
        pending["deciding"] = True
        try:
            # Recheck ownership and liveness in the thread before posting a decision.
            path = f"/api/threads/{quote(self.thread, safe='')}/events"
            result = await self.nano.request("GET", path, params={"limit": "1000"})
            if self.pending_approval is not pending or self.task is None or self.task.done():
                return
            live = next((event for event in result.get("events", [])
                         if event.get("id") == pending["event_id"]
                         and event.get("thread", self.thread) == self.thread
                         and event.get("type") == "approval" and event.get("status") == "pending"
                         and event.get("source") != "background"), None)
            if live is None:
                await self.clear_approval("此请求已处理或已失效")
                return
            if approval_details(live) != (pending["text"], pending["can_approve"]):
                await self.clear_approval("操作详情已改变，请重新查看")
                return
            await self.nano.request("POST", f"/api/approvals/{quote(pending['event_id'], safe='')}",
                                    json={"approved": approved, "scope": "once"})
            self.handled_approvals.add(pending["event_id"])
            if self.pending_approval is pending:
                await self.clear_approval("已允许一次" if approved else "已拒绝")
        except TurnError as exc:
            if self.pending_approval is pending:
                if isinstance(exc, UpstreamHTTPError) and exc.status == 404:
                    await self.clear_approval("此请求已处理或已失效")
                else:
                    await self.approval_card(pending, "发送失败，请重试或在网页查看")
        finally:
            pending["deciding"] = False

    async def send(self, kind: str, text: str | None = None, turn_id: int | None = None):
        message = {"type": kind, "id": turn_id if turn_id is not None else self.id}
        if kind == "progress":
            self.progress_seq += 1
            message["seq"] = self.progress_seq
        if text is not None:
            message["text"] = trim_utf8(text)
        wire = json.dumps(message, ensure_ascii=False)
        # JSON escaping can expand control characters sixfold. Bound the wire
        # message as well as UTF-8 text, matching the firmware's receive limit.
        raw = message.get("text", "").encode("utf-8")
        while len(wire.encode("utf-8")) > MAX_JSON and raw:
            raw = raw[: len(raw) * 3 // 4]
            message["text"] = raw.decode("utf-8", "ignore") + "\n[回复较长，请在电脑查看全文]"
            wire = json.dumps(message, ensure_ascii=False)
        await self.ws.send_str(wire)

    async def cancel(self, preserve_question=False):
        await self.clear_approval()
        self.handled_approvals.clear()
        self.pcm = None
        task, self.task = self.task, None
        active = task is not None and not task.done()
        keep_question = preserve_question and self.awaiting_answer
        if active:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, ClientError, ConnectionError):
                await task
        if ((active and not self.reply_complete) or self.awaiting_answer) and not keep_question:
            if self.thread:
                await self.nano.stop(self.thread)
            # A cancelled generation cannot append a late answer to the next turn.
            self.thread = None
        self.awaiting_answer = keep_question
        self.id = None

    async def transcribe(self, audio: bytes) -> str | None:
        config = self.config.get("asr", {})
        url = config.get("url", "")
        if not url:
            return None
        provider = speech_provider(config)
        body = FormData()
        body.add_field("audio" if provider == "blackbox" else "file", audio,
                       filename="recording.wav", content_type="audio/wav")
        body.add_field("model_name" if provider == "blackbox" else "model",
                       config.get("model", "sensevoice" if provider == "blackbox" else "whisper-1"))
        if provider == "openai":
            body.add_field("response_format", "json")
        if config.get("language"):
            body.add_field("language", config["language"])
        key = os.environ.get(config.get("key_env", "ASR_API_KEY"), "")
        headers = {"Authorization": "Bearer " + key} if key else {}
        try:
            async with self.http.post(url, data=body, headers=headers, timeout=ClientTimeout(total=60)) as response:
                if response.status != 200:
                    raise TurnError(f"语音识别失败 (HTTP {response.status})")
                data = await read_object(response, 65536)
        except (ClientError, OSError, asyncio.TimeoutError, ValueError) as exc:
            raise TurnError("无法连接语音识别服务") from exc
        return transcript_text(data, provider)

    async def synthesize(self, text: str) -> bytes | None:
        config = self.config.get("tts", {})
        if not config.get("url"):
            return None
        if not shutil.which("ffmpeg"):
            raise TurnError("电脑未安装 ffmpeg，暂时无法播报")
        key = os.environ.get(config.get("key_env", "TTS_API_KEY"), "")
        headers = {"Authorization": "Bearer " + key} if key else {}
        provider = speech_provider(config)
        if provider == "blackbox":
            # JSON preserves booleans (some backend form handlers treat the
            # string 'false' as true). Let the service own voice references.
            settings = dict(config.get("parameters", {}))
            settings.update({"model_name": config.get("model", "voxcpmtts"),
                             "streaming": False, "streaming_mode": False, "media_type": "wav"})
            if "speed" in config:
                settings["speed"] = config["speed"]
            body = {"text": trim_utf8(text), "settings": settings}
        else:
            body = {"model": config.get("model", "tts-1"), "voice": config.get("voice", "alloy"),
                    "input": trim_utf8(text), "response_format": "wav"}
            if "speed" in config:
                body["speed"] = config["speed"]
        try:
            async with self.http.post(config["url"], json=body, headers=headers,
                                      timeout=ClientTimeout(total=60)) as response:
                if response.status != 200:
                    raise TurnError(f"语音合成失败 (HTTP {response.status})，文字回复仍可阅读")
                audio = bytearray()
                async for chunk in response.content.iter_chunked(16384):
                    audio.extend(chunk)
                    if len(audio) > MAX_TTS_WAV:
                        raise TurnError("语音合成文件过大，文字回复仍可阅读")
        except (ClientError, OSError, asyncio.TimeoutError, ValueError) as exc:
            raise TurnError("无法连接语音合成服务，文字回复仍可阅读") from exc
        # The board's duplex bus stays at 16 kHz. Convert on the computer;
        # accepting only WAV also prevents ffmpeg from fetching external URLs.
        process = await asyncio.create_subprocess_exec(
            "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error",
            "-protocol_whitelist", "pipe", "-f", "wav", "-i", "pipe:0",
            "-t", "61", "-ac", "1", "-ar", str(RATE), "-f", "s16le",
            "-fs", str(MAX_SPEECH + 2), "pipe:1",
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            pcm, _ = await asyncio.wait_for(process.communicate(bytes(audio)), 15)
        except BaseException:
            if process.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    process.kill()
            await process.wait()
            raise
        if process.returncode or not pcm or len(pcm) % 2:
            raise TurnError("语音服务未返回有效 WAV 音频，文字回复仍可阅读")
        if len(pcm) > MAX_SPEECH:
            raise TurnError("播报超过 60 秒上限，文字回复仍可阅读")
        return pcm

    async def synthesize_chunks(self, text: str):
        emitted = False
        config = self.config.get("tts", {})
        try:
            async with contextlib.aclosing(self._synthesize_chunks(text)) as chunks:
                async for audio in chunks:
                    emitted = True
                    yield audio
        except TurnError:
            if (emitted or speech_provider(config) != "blackbox"
                    or config.get("model", "voxcpmtts") != "voxcpmtts"
                    or not config.get("audio_streaming", True)):
                raise
            LOG.warning("VoxCPM streaming unavailable; retrying phrase with WAV output")
            audio = await self.synthesize(text)
            if audio:
                yield audio

    async def _synthesize_chunks(self, text: str):
        """VoxCPM: consume the WAV header, then resample PCM without waiting for EOF."""
        config = self.config.get("tts", {})
        if (speech_provider(config) != "blackbox" or config.get("model", "voxcpmtts") != "voxcpmtts"
                or not config.get("audio_streaming", True)):
            audio = await self.synthesize(text)
            if audio:
                yield audio
            return
        if not config.get("url"):
            return
        if not shutil.which("ffmpeg"):
            raise TurnError("电脑未安装 ffmpeg，暂时无法播报")
        settings = dict(config.get("parameters", {}))
        settings.update({"model_name": "voxcpmtts", "streaming": True,
                         "streaming_mode": False, "media_type": "wav"})
        if "speed" in config:
            settings["speed"] = config["speed"]
        key = os.environ.get(config.get("key_env", "TTS_API_KEY"), "")
        headers = {"Authorization": "Bearer " + key} if key else {}
        process = feeder = None
        try:
            async with asyncio.timeout(75):
                async with self.http.post(config["url"], headers=headers,
                        json={"text": trim_utf8(text), "settings": settings},
                        timeout=ClientTimeout(total=60)) as response:
                    if response.status != 200:
                        raise TurnError(f"语音合成失败 (HTTP {response.status})，文字回复仍可阅读")
                    stream = response.content
                    header = await stream.readexactly(12)
                    if header[:4] != b"RIFF" or header[8:] != b"WAVE":
                        raise TurnError("流式语音未返回 WAV 音频")
                    fmt = None
                    header_bytes = 12
                    while True:
                        chunk, size = struct.unpack("<4sI", await stream.readexactly(8))
                        header_bytes += 8
                        if chunk == b"data":
                            if fmt is None:
                                raise TurnError("流式 WAV 缺少格式信息")
                            break
                        header_bytes += size + (size % 2)
                        if header_bytes > 65536:
                            raise TurnError("流式 WAV 头过大")
                        payload = await stream.readexactly(size + (size % 2))
                        if chunk == b"fmt " and size >= 16:
                            encoding, channels, rate, _, align, bits = struct.unpack_from("<HHIIHH", payload)
                            if encoding != 1 or channels not in (1, 2) or bits != 16 or align != channels * 2 or not 8000 <= rate <= 96000:
                                raise TurnError("流式 WAV 必须是 16-bit PCM 音频")
                            fmt = (channels, rate)
                    channels, rate = fmt
                    process = await asyncio.create_subprocess_exec(
                        "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error",
                        "-probesize", "32", "-analyzeduration", "0", "-f", "s16le",
                        "-ar", str(rate), "-ac", str(channels), "-i", "pipe:0",
                        "-ac", "1", "-ar", str(RATE), "-f", "s16le", "-flush_packets", "1", "pipe:1",
                        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.DEVNULL)

                    async def feed():
                        received = 0
                        try:
                            # VoxCPM's streaming WAV has data size=0; read until
                            # EOF. A finite WAV instead stops at its data boundary.
                            remaining = size if size not in (0, 0xFFFFFFFF) else None
                            while remaining is None or remaining:
                                audio = await stream.read(min(2048, remaining) if remaining is not None else 2048)
                                if not audio:
                                    if remaining:
                                        raise TurnError("流式 WAV 音频不完整")
                                    break
                                received += len(audio)
                                if received > MAX_TTS_WAV:
                                    raise TurnError("语音合成文件过大")
                                process.stdin.write(audio)
                                await process.stdin.drain()
                                if remaining is not None:
                                    remaining -= len(audio)
                            if received % (channels * 2):
                                raise TurnError("流式 PCM 数据不完整")
                        finally:
                            process.stdin.close()

                    feeder = asyncio.create_task(feed())
                    received = 0
                    pending = b""
                    while audio := await process.stdout.read(2048):
                        pending += audio
                        end = len(pending) & ~1
                        if end:
                            received += end
                            if received > MAX_SPEECH:
                                raise TurnError("播报超过 60 秒上限，文字回复仍可阅读")
                            yield pending[:end]
                            pending = pending[end:]
                    await feeder
                    if await process.wait() or pending or not received:
                        raise TurnError("流式音频转换失败，文字回复仍可阅读")
        except (ClientError, OSError, asyncio.TimeoutError, asyncio.IncompleteReadError) as exc:
            raise TurnError("流式语音连接中断，文字回复仍可阅读") from exc
        finally:
            if feeder:
                feeder.cancel()
            if process and process.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    process.kill()
            if feeder:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await feeder
            if process:
                await process.wait()

    async def speak(self, text: str, turn_id: int):
        try:
            pcm = await self.synthesize(text)
        except (TurnError, OSError, asyncio.TimeoutError) as exc:
            warning = str(exc) if isinstance(exc, TurnError) else "音频转换失败，文字回复仍可阅读"
            await self.send("warning", warning, turn_id)
            return
        if pcm is None:
            return
        await self.ws.send_json({"type": "audio", "id": turn_id, "rate": RATE,
                                 "channels": 1, "format": "s16le", "frames": len(pcm) // 2})
        for offset in range(0, len(pcm), 2048):
            await self.ws.send_bytes(struct.pack("<I", turn_id) + pcm[offset:offset + 2048])
        await self.send("audio_end", turn_id=turn_id)

    async def streaming_reply(self, text: str, turn_id: int) -> str:
        """Generate and synthesize concurrently; one writer keeps PCM in order."""
        queue = asyncio.Queue(maxsize=64)
        accumulated = ""
        pending = ""
        emitted = ""
        sent_bytes = 0
        started = False
        failed = False

        lock = asyncio.Lock()
        pending_at = 0.0
        turn_at = time.monotonic()
        first_delta = True

        async def flush(force=False):
            nonlocal pending, emitted, pending_at
            while pending:
                # Short first phrase, then longer phrases for smoother prosody.
                limit = 24 if not emitted else 48
                match = next((m for m in re.finditer(r"[。！？!?；;，,：:\n]|[.](?=\s)", pending)
                              if m.end() >= 6 or m.group()[0] in "。！？!?；;\n"), None)
                end = match.end() if match else 0
                if not end and len(pending) >= limit:
                    space = pending.rfind(" ", 6, limit + 1)
                    end = space + 1 if space >= 6 else limit
                if not end and force and len(pending.strip()) >= 6:
                    end = len(pending)
                if not end:
                    break
                phrase = pending[:end]
                if phrase.strip():
                    await queue.put(phrase)
                emitted += phrase
                pending = pending[end:]
                pending_at = time.monotonic() if pending else 0
                await self.send("reply", accumulated, turn_id)

        async def delta(part):
            nonlocal accumulated, pending, pending_at, first_delta
            async with lock:
                if first_delta:
                    LOG.info("turn=%s first_text=%.3fs", turn_id, time.monotonic() - turn_at)
                    first_delta = False
                part = part.encode()[:max(0, MAX_REPLY - len(accumulated.encode()))].decode("utf-8", "ignore")
                if part and not pending:
                    pending_at = time.monotonic()
                accumulated += part
                pending += part
                await flush()

        async def timer():
            while True:
                await asyncio.sleep(0.05)
                async with lock:
                    delay = 0.35 if not emitted else 0.6
                    if pending_at and time.monotonic() - pending_at >= delay:
                        await flush(force=True)

        async def speaker():
            nonlocal sent_bytes, started, failed
            while (sentence := await queue.get()) is not None:
                if failed:
                    continue
                try:
                    caption_sent = False
                    async with contextlib.aclosing(self.synthesize_chunks(sentence)) as chunks:
                        async for audio in chunks:
                            if sent_bytes + len(audio) > MAX_SPEECH:
                                raise TurnError("播报超过 60 秒上限，文字回复仍可阅读")
                            if not started:
                                LOG.info("turn=%s first_audio=%.3fs", turn_id, time.monotonic() - turn_at)
                                await self.ws.send_json({"type": "audio", "id": turn_id, "rate": RATE,
                                                         "channels": 1, "format": "s16le",
                                                         "frames": MAX_SPEECH // 2, "stream": True})
                                started = True
                            if not caption_sent:
                                await self.ws.send_json({"type": "caption", "id": turn_id,
                                                         "frame": sent_bytes // 2, "text": sentence})
                                caption_sent = True
                            for offset in range(0, len(audio), 2048):
                                await self.ws.send_bytes(struct.pack("<I", turn_id) + audio[offset:offset + 2048])
                            sent_bytes += len(audio)
                except (TurnError, OSError, asyncio.TimeoutError) as exc:
                    failed = True
                    LOG.warning("turn=%s reason=tts_failed", turn_id)
                    warning = str(exc) if isinstance(exc, TurnError) else "音频转换失败，文字回复仍可阅读"
                    await self.send("warning", warning, turn_id)

        worker = asyncio.create_task(speaker())
        flusher = asyncio.create_task(timer())
        try:
            answer = await self.nano.reply(self.thread, text, self.send, on_delta=delta,
                                           on_approvals=self.sync_approvals,
                                           on_question=self.question_ready, on_images=self.collect_images)
            LOG.info("turn=%s final_text=%.3fs", turn_id, time.monotonic() - turn_at)
            flusher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await flusher
            # No /ws or no deltas: synthesize the final answer as before. Do not
            # speak an already emitted prefix twice when the final event arrives.
            if answer.startswith(emitted):
                tail = answer[len(emitted):]
            elif not emitted:
                tail = answer
            else:
                tail = answer  # a tool step may have a separate final summary
            await self.send("reply", answer, turn_id)
            # Even a final-only upstream gets short, bounded phrase captions.
            async with lock:
                accumulated = answer
                pending = tail
                await flush(force=True)
                if pending.strip():
                    await queue.put(pending)
                pending = ""
            await queue.put(None)
            await worker
            if started:
                await self.send("audio_end", turn_id=turn_id)
                LOG.info("turn=%s audio_end=%.3fs audio_frames=%s", turn_id,
                         time.monotonic() - turn_at, sent_bytes // 2)
            return answer
        finally:
            flusher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await flusher
            worker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker

    def save_recording(self, audio: bytes, turn_id: int):
        directory = self.config.get("gateway", {}).get("recordings_dir", "")
        if not directory:
            return
        folder = Path(directory)
        folder.mkdir(parents=True, exist_ok=True, mode=0o700)
        # Bounded retention. No client-provided filenames or paths.
        limit = max(1, min(int(self.config.get("gateway", {}).get("recordings_keep", 20)), 1000))
        files = sorted(folder.glob("cores3-*.wav"), key=lambda p: p.stat().st_mtime_ns)
        for old in files[:max(0, len(files) - limit + 1)]:
            old.unlink()
        name = folder / f"cores3-{time.time_ns()}-{turn_id}.wav"
        with name.open("xb") as f:
            os.chmod(name, 0o600)
            f.write(audio)

    async def run_turn(self, turn_id: int, text: str | None, pcm: bytes | None):
        try:
            # Includes ASR, model work, queued TTS and image delivery; activity
            # cannot keep a broken turn alive forever.
            async with asyncio.timeout(self.nano.max_timeout):
                await self._run_turn(turn_id, text, pcm)
        except asyncio.TimeoutError:
            if self.reply_complete:
                LOG.warning("turn=%s reason=image_deadline", turn_id)
                return  # A best-effort preview cannot invalidate a delivered reply.
            LOG.warning("turn=%s reason=turn_deadline", turn_id)
            await self.send("error", "本次处理超时（达到整轮最长时限）；请缩短任务或在网页继续", turn_id)
            if self.thread and not self.awaiting_answer:
                await self.nano.stop(self.thread)
                self.thread = None

    async def _run_turn(self, turn_id: int, text: str | None, pcm: bytes | None):
        try:
            if pcm is not None:
                audio = wav_bytes(pcm)
                self.save_recording(audio, turn_id)
                text = await self.transcribe(audio)
                if text is None:
                    await self.send("sent")
                    saved = bool(self.config.get("gateway", {}).get("recordings_dir"))
                    await self.send("reply", f"已收到 {len(pcm) / (RATE * 2):.1f} 秒录音。"
                                    + ("WAV 已保存到电脑。" if saved else "未启用录音保存。")
                                    + "语音识别尚未配置；目前可以通过 USB 发送文字。")
                    await self.send("done")
                    return
                await self.send("heard", text)
            if self.thread is None:
                self.thread = await self.nano.create_thread()
            self.awaiting_answer = False
            if pcm is not None and self.want_audio and self.streaming_audio:
                await self.streaming_reply(text, turn_id)
            else:
                answer = await self.nano.reply(self.thread, text, self.send, on_approvals=self.sync_approvals,
                                               on_question=self.question_ready, on_images=self.collect_images)
                await self.send("reply", answer)
                if pcm is not None and self.want_audio:
                    await self.speak(answer, turn_id)
            self.reply_complete = True
            await self.send("done")
            LOG.info("turn=%s done", turn_id)
            await self.show_image(turn_id)
        except TurnError as exc:
            await self.send("error", str(exc))
            if self.thread and not self.awaiting_answer:
                await self.nano.stop(self.thread)
                self.thread = None
        except (OSError, ValueError, TypeError, KeyError):
            await self.send("error", "网关处理失败，请检查本地配置及录音目录")
            if self.thread and not self.awaiting_answer:
                await self.nano.stop(self.thread)
                self.thread = None
        finally:
            await self.clear_approval()

    async def message(self, message):
        if message.type == WSMsgType.BINARY:
            data = message.data
            if len(data) < 6 or len(data) > 2052 or (len(data) - 4) % 2:
                raise TurnError("无效的 PCM 数据包")
            turn_id = struct.unpack_from("<I", data)[0]
            if turn_id != self.id:
                return  # stale packet from an explicitly cancelled turn
            if self.pcm is None:
                raise TurnError("请先开始录音")
            if time.monotonic() > self.upload_deadline:
                raise TurnError("录音上传超时")
            if len(self.pcm) + len(data) - 4 > MAX_AUDIO:
                raise TurnError("录音超过 16 秒上限")
            self.pcm.extend(data[4:])
            return
        if message.type != WSMsgType.TEXT:
            return
        try:
            data = json.loads(message.data)
        except ValueError as exc:
            raise TurnError("无效的 JSON") from exc
        if not isinstance(data, dict):
            raise TurnError("消息必须是 JSON 对象")
        kind, turn_id = data.get("type"), data.get("id")
        if kind == "subscribe":
            if data.get("notifications") is True:
                self.inbox.subscribe(self)
                await self.inbox.publish()
            return
        if kind == "notification_read":
            try:
                await self.inbox.read(self, data.get("token"))
            except OSError:
                LOG.warning("Notification read receipt could not be saved")
            return
        if not valid_id(turn_id):
            raise TurnError("无效的请求 ID")
        if kind == "approval_decision":
            await self.decide_approval(data)
            return
        if kind == "cancel":
            if turn_id == self.id:
                await self.cancel()
            return
        if kind == "end":
            if turn_id != self.id:
                return
            if self.pcm is None or not self.pcm:
                raise TurnError("没有录音")
            audio, self.pcm = bytes(self.pcm), None
            self.task = asyncio.create_task(self.run_turn(turn_id, None, audio))
            return
        if kind not in ("text", "begin"):
            raise TurnError("未知消息类型")
        if turn_id in self.used_ids or len(self.used_ids) >= 10000:
            raise TurnError("请求 ID 重复或连接已用尽，请重新连接")
        # Validate before cancelling a valid current turn.
        if kind == "text":
            text = data.get("text")
            if not isinstance(text, str) or not text.strip() or len(text.encode()) > MAX_TEXT:
                raise TurnError("文字为空或超过 4000 字节")
        elif (data.get("rate"), data.get("channels"), data.get("format")) != (RATE, 1, "s16le"):
            raise TurnError("只支持 16 kHz 单声道 s16le")
        await self.cancel(preserve_question=True)
        self.id = turn_id
        self.approval_capable = data.get("approvals") is True
        self.image_size = display_size(data.get("images"))
        self.image_paths = []
        self.reply_complete = False
        self.progress_seq = 0
        self.used_ids.add(turn_id)
        if kind == "begin":
            self.want_audio = data.get("tts") is True
            self.streaming_audio = data.get("streaming") is True
            self.pcm = bytearray()
            self.upload_deadline = time.monotonic() + UPLOAD_TIMEOUT
        else:
            self.task = asyncio.create_task(self.run_turn(turn_id, text.strip(), None))

    async def loop(self):
        await self.ws.send_json({"type": "hello", "protocol": 1, "audio": "s16le/16000/mono",
                                 "approvals": True,
                                 "images": self.app_images.enabled,
                                 "notifications": self.inbox.enabled,
                                 "tts": bool(self.config.get("tts", {}).get("url"))})
        try:
            while not self.ws.closed:
                try:
                    msg = await asyncio.wait_for(self.ws.receive(), timeout=1)
                except asyncio.TimeoutError:
                    if self.pcm is not None and time.monotonic() > self.upload_deadline:
                        await self.send("error", "录音上传超时")
                        await self.cancel()
                    continue
                if msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR):
                    break
                try:
                    await self.message(msg)
                except TurnError as exc:
                    await self.send("error", str(exc))
                    await self.cancel()
        finally:
            self.inbox.sessions.discard(self)
            await self.cancel()


CONFIG = web.AppKey("config", dict)
HTTP = web.AppKey("http", ClientSession)
NANO = web.AppKey("nano", NanoMuse)
INBOX = web.AppKey("inbox", NotificationInbox)
IMAGES = web.AppKey("images", ImageBridge)
CONNECTIONS = web.AppKey("connections", set)


def create_app(config: dict) -> web.Application:
    for section in ("asr", "tts"):
        speech_provider(config.get(section, {}))
    parameters = config.get("tts", {}).get("parameters", {})
    if not isinstance(parameters, dict):
        raise ValueError("tts.parameters must be a TOML table")
    key = os.environ.get(config.get("gateway", {}).get("key_env", "CORES3_DEVICE_KEY"), "")
    if not 16 <= len(key) <= 127 or any(ord(c) < 33 or ord(c) > 126 for c in key):
        raise ValueError("CORES3_DEVICE_KEY must contain 16–127 printable ASCII characters without spaces")
    app = web.Application(client_max_size=MAX_JSON)
    app[CONFIG] = config
    # Compare digests: fixed size, no timing leak from differing credential lengths.
    expected = hashlib.sha256(("Bearer " + key).encode()).digest()

    async def start(app):
        app[HTTP] = ClientSession()
        app[NANO] = NanoMuse(app[HTTP], config.get("nanomuse", {}))
        app[IMAGES] = ImageBridge(app[NANO], config.get("images", {}))
        app[INBOX] = NotificationInbox(app[NANO], config.get("notifications", {}), TurnError)
        poller = asyncio.create_task(app[INBOX].run())
        try:
            yield
        finally:
            poller.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await poller
            await app[HTTP].close()
    app.cleanup_ctx.append(start)
    app[CONNECTIONS] = set()

    async def device(request):
        auth = request.headers.get("Authorization", "")
        if not hmac.compare_digest(hashlib.sha256(auth.encode()).digest(), expected):
            raise web.HTTPUnauthorized(text="device key required")
        if request.headers.get("Origin"):
            raise web.HTTPForbidden(text="device endpoint only")
        if len(app[CONNECTIONS]) >= 4:
            raise web.HTTPServiceUnavailable(text="connection limit")
        ws = web.WebSocketResponse(max_msg_size=MAX_JSON, heartbeat=20)
        app[CONNECTIONS].add(ws)
        try:
            await ws.prepare(request)
            await Session(ws, app).loop()
        finally:
            app[CONNECTIONS].discard(ws)
        return ws

    async def health(request):
        return web.json_response({"ok": True, "protocol": 1})

    async def shutdown(app):
        await asyncio.gather(*(ws.close(code=1001) for ws in list(app[CONNECTIONS])))
    app.on_shutdown.append(shutdown)
    app.router.add_get("/health", health)
    app.router.add_get("/device", device)
    return app


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("config.toml"))
    args = parser.parse_args()
    with args.config.open("rb") as f:
        config = tomllib.load(f)
    settings = config.get("gateway", {})
    app = create_app(config)
    # Device keys, tokens, and message contents never appear in access logs.
    web.run_app(app, host=settings.get("host", "0.0.0.0"), port=int(settings.get("port", 8765)), access_log=None)


if __name__ == "__main__":
    main()
