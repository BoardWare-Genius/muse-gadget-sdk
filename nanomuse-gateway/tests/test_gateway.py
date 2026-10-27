import asyncio
import contextlib
import io
import json
import os
from pathlib import Path
import shutil
import struct
import sys
import tempfile
import unittest
from unittest.mock import patch
import wave

from aiohttp import WSServerHandshakeError, WSMsgType, web
from aiohttp.test_utils import TestClient, TestServer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from gateway import MAX_AUDIO, MAX_JSON, NANO, Session, create_app, trim_utf8

KEY = "test-device-key-at-least-16-chars"
AUTH = {"Authorization": "Bearer " + KEY}


class GatewayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.env = patch.dict(os.environ, {"CORES3_DEVICE_KEY": KEY, "NANOMUSE_SERVER_TOKEN": "private-server-key"})
        self.env.start()
        self.counter = 0
        self.threads = {}
        self.stopped = []
        self.polls = {}
        self.final_after = 2
        self.fail_send = False
        self.asr_audio = None
        self.asr_status = 200
        self.asr_auth = None
        self.tts_requests = []
        self.tts_status = 200
        self.tts_delay = 0
        self.tts_invalid = False
        self.stream_tts = False
        self.reject_stream = False
        self.allow_tts_end = asyncio.Event()
        self.blackbox_result = {"text": "本地识别测试"}
        self.blackbox_forms = []
        self.chunked = False
        self.live = []
        self.disable_live = False
        self.allow_final = asyncio.Event()
        self.stream_parts = None
        self.tts_started = asyncio.Event()
        self.stream_jobs = []
        self.early_tts = False
        self.temp = tempfile.TemporaryDirectory()

        async def create(request):
            self.assertEqual(request.headers["Authorization"], "Bearer private-server-key")
            self.counter += 1
            thread = str(self.counter)
            self.threads[thread] = [{"id": "old", "type": "assistant", "final": True, "text": "OLD REPLY"}]
            return web.json_response({"id": thread})

        async def send(request):
            if self.fail_send:
                return web.json_response({}, status=401)
            thread = request.match_info["thread"]
            body = await request.json()
            self.assertEqual(body["language"], "zh-CN")
            event = {"id": "user-" + str(len(self.threads[thread])), "type": "user", "text": body["text"]}
            self.threads[thread].append(event)
            self.threads[thread].append({"id": "answer-" + event["id"], "thread": thread,
                                         "type": "assistant", "text": "最终答复：" + body["text"], "final": False})
            self.polls[thread] = 0
            if self.stream_parts:
                self.threads[thread][-1]["text"] = "".join(self.stream_parts)
                async def stream():
                    for index, part in enumerate(self.stream_parts):
                        for live in self.live:
                            await live.send_json({"kind": "delta", "thread": thread, "text": part})
                        if index == 0:
                            await self.tts_started.wait()
                            await self.allow_final.wait()
                    self.threads[thread][-1]["final"] = True
                self.stream_jobs.append(asyncio.create_task(stream()))
            return web.json_response({"event": event, "thread": {"id": thread}})

        async def events(request):
            thread = request.match_info["thread"]
            self.polls[thread] += 1
            # nanoMuse can patch an existing narration event to final.
            if not self.stream_parts and self.polls[thread] >= self.final_after:
                self.threads[thread][-1]["final"] = True
            result = {"events": self.threads[thread]}
            if self.chunked:
                response = web.StreamResponse(headers={"Content-Type": "application/json"})
                await response.prepare(request)
                data = json.dumps(result, ensure_ascii=False).encode()
                await response.write(data[:15])
                await asyncio.sleep(0.005)
                await response.write(data[15:])
                await response.write_eof()
                return response
            return web.json_response(result)

        async def stop(request):
            self.stopped.append(request.match_info["thread"])
            return web.json_response({"ok": True})

        async def asr(request):
            self.asr_auth = request.headers.get("Authorization")
            form = await request.post()
            self.assertEqual(form["model"], "test-asr")
            self.asr_audio = form["file"].file.read()
            return web.json_response({"text": "麦克风测试"}, status=self.asr_status)

        async def tts(request):
            self.assertEqual(request.headers.get("Authorization"), "Bearer test-tts-key")
            self.tts_requests.append(await request.json())
            self.early_tts |= any(not events[-1].get("final") for events in self.threads.values())
            self.tts_started.set()
            if self.reject_stream and self.tts_requests[-1].get("settings", {}).get("streaming"):
                return web.Response(status=503)
            await asyncio.sleep(self.tts_delay)
            if self.tts_invalid:
                return web.Response(body=b"not audio")
            out = io.BytesIO()
            with wave.open(out, "wb") as wav:
                wav.setnchannels(2)
                wav.setsampwidth(2)
                wav.setframerate(24000)
                wav.writeframes(struct.pack("<hh", 1234, 1234) * 2400)
            if self.stream_tts:
                self.assertIs(self.tts_requests[-1]["settings"]["streaming"], True)
                response = web.StreamResponse(headers={"Content-Type": "audio/wav"})
                await response.prepare(request)
                # Streaming VoxCPM WAV: zero-sized data header, fragmented
                # across arbitrary HTTP chunks; stereo 24k resampled to 16k.
                payload = bytearray(out.getvalue())
                payload[4:8] = struct.pack("<I", 36)
                payload[40:44] = b"\x00" * 4
                await response.write(payload[:7])
                await response.write(payload[7:44])
                await response.write(payload[44:] * 10)
                await self.allow_tts_end.wait()
                await response.write_eof()
                return response
            return web.Response(body=out.getvalue(), status=self.tts_status, content_type="audio/wav")

        async def blackbox_asr(request):
            form = await request.post()
            self.assertIn(form["model_name"], ("sensevoice", "qwen", "fun-asr"))
            self.assertEqual(form["language"], "Chinese")
            self.assertNotIn("file", form)
            self.assertNotIn("model", form)
            self.assertNotIn("response_format", form)
            self.asr_audio = form["audio"].file.read()
            self.blackbox_forms.append(form["model_name"])
            return web.json_response(self.blackbox_result)

        async def live_events(request):
            if self.disable_live:
                raise web.HTTPNotFound()
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            self.assertEqual(await ws.receive_json(), {"kind": "auth", "token": "private-server-key"})
            self.live.append(ws)
            await ws.send_json({"kind": "hello"})
            async for _ in ws:
                pass
            self.live.remove(ws)
            return ws

        upstream = web.Application()
        upstream.router.add_get("/ws", live_events)
        upstream.router.add_post("/api/threads", create)
        upstream.router.add_post("/api/threads/{thread}/send", send)
        upstream.router.add_get("/api/threads/{thread}/events", events)
        upstream.router.add_post("/api/threads/{thread}/stop", stop)
        upstream.router.add_post("/asr", asr)
        upstream.router.add_post("/tts", tts)
        upstream.router.add_post("/asr-blackbox", blackbox_asr)
        self.server = TestServer(upstream)
        await self.server.start_server()
        self.config = {"nanomuse": {"url": str(self.server.make_url("")), "poll_interval": 0.01, "turn_timeout": 1}}
        self.client = TestClient(TestServer(create_app(self.config)))
        await self.client.start_server()

    async def asyncTearDown(self):
        self.allow_tts_end.set()
        await self.client.close()
        for task in self.stream_jobs:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await self.server.close()
        self.temp.cleanup()
        self.env.stop()

    async def connect(self):
        ws = await self.client.ws_connect("/device", headers=AUTH)
        self.assertEqual((await ws.receive_json())["protocol"], 1)
        return ws

    async def until_done(self, ws):
        result = []
        for _ in range(10):
            event = await asyncio.wait_for(ws.receive_json(), 3)
            result.append(event)
            if event["type"] in ("done", "error"):
                return result
        self.fail("no terminal event")

    async def upload(self, ws, turn_id, pcm, tts=False, streaming=False):
        await ws.send_json({"type": "begin", "id": turn_id, "rate": 16000, "channels": 1,
                            "format": "s16le", "tts": tts, "streaming": streaming})
        for offset in range(0, len(pcm), 2048):
            await ws.send_bytes(struct.pack("<I", turn_id) + pcm[offset:offset + 2048])
        await ws.send_json({"type": "end", "id": turn_id})

    async def test_auth_and_origin_rejected_health_is_public(self):
        response = await self.client.get("/health")
        self.assertEqual((await response.json())["protocol"], 1)
        for headers in ({}, {"Authorization": "Bearer private-server-key"}, {**AUTH, "Origin": "https://example.org"}):
            with self.assertRaises(WSServerHandshakeError):
                await self.client.ws_connect("/device", headers=headers)

    async def test_text_ignores_old_final_and_waits_for_final_patch(self):
        ws = await self.connect()
        for turn_id, text in [(1, "你好"), (2, "继续")]:
            await ws.send_json({"type": "text", "id": turn_id, "text": text})
            events = await self.until_done(ws)
            self.assertEqual([e["type"] for e in events], ["sent", "reply", "done"])
            self.assertEqual(events[1]["text"], "最终答复：" + text)
            self.assertTrue(all(e["id"] == turn_id for e in events))
        self.assertEqual(self.counter, 1)  # keep context across completed turns
        await ws.close()

    async def test_chunked_http_response_waits_for_complete_json(self):
        self.chunked = True
        ws = await self.connect()
        await ws.send_json({"type": "text", "id": 1, "text": "分块传输"})
        events = await self.until_done(ws)
        self.assertEqual(events[-1]["type"], "done")
        self.assertEqual(events[1]["text"], "最终答复：分块传输")
        await ws.close()

    async def test_duplicate_id_and_invalid_json_rejected(self):
        ws = await self.connect()
        await ws.send_json({"type": "text", "id": 1, "text": "one"})
        await self.until_done(ws)
        await ws.send_json({"type": "text", "id": 1, "text": "reused"})
        self.assertEqual((await ws.receive_json())["type"], "error")
        await ws.send_str("[")
        self.assertEqual((await ws.receive_json())["type"], "error")
        await ws.close()

    async def test_recording_without_asr_saves_valid_wav_and_says_unconfigured(self):
        self.config["gateway"] = {"recordings_dir": self.temp.name, "recordings_keep": 1}
        ws = await self.connect()
        pcm = struct.pack("<h", -1234) * 16000
        for turn_id in (1, 2):
            await self.upload(ws, turn_id, pcm)
            events = await self.until_done(ws)
            self.assertIn("语音识别尚未配置", events[1]["text"])
            self.assertEqual(events[-1]["type"], "done")
        files = list(Path(self.temp.name).glob("*.wav"))
        self.assertEqual(len(files), 1)
        with wave.open(str(files[0])) as wav:
            self.assertEqual((wav.getframerate(), wav.getnchannels(), wav.getsampwidth()), (16000, 1, 2))
            self.assertEqual(wav.readframes(16000), pcm)
        self.assertEqual(self.counter, 0)  # no invented transcript sent to the agent
        await ws.close()

    async def test_asr_multipart_routes_transcript_to_nanomuse(self):
        self.config["asr"] = {"url": str(self.server.make_url("/asr")), "model": "test-asr"}
        ws = await self.connect()
        pcm = b"\x01\x00" * 8000
        await self.upload(ws, 22, pcm)
        events = await self.until_done(ws)
        self.assertEqual([e["type"] for e in events], ["heard", "sent", "reply", "done"])
        self.assertEqual(events[0]["text"], "麦克风测试")
        with wave.open(io.BytesIO(self.asr_audio)) as wav:
            self.assertEqual(wav.readframes(8000), pcm)
        await ws.close()

    async def test_blackbox_asr_models_and_sensevoice_segments(self):
        self.config["asr"] = {"provider": "blackbox", "url": str(self.server.make_url("/asr-blackbox")),
                              "language": "Chinese"}
        cases = [("qwen", {"text": "千问识别"}, "千问识别"),
                 ("fun-asr", {"status": "success", "text": "语音识别"}, "语音识别"),
                 ("sensevoice", {"result": [{"text": "raw emotion text", "clean_text": "第一句"},
                                            {"text": "第二句"}]}, "第一句\n第二句")]
        for model, response, expected in cases:
            self.config["asr"]["model"] = model
            self.blackbox_result = response
            ws = await self.connect()
            await self.upload(ws, 1, b"\x01\x00" * 800)
            events = await self.until_done(ws)
            self.assertEqual(events[0]["text"], expected)
            self.assertEqual(events[-1]["type"], "done")
            with wave.open(io.BytesIO(self.asr_audio)) as wav:
                self.assertEqual(wav.getframerate(), 16000)
                self.assertEqual(wav.getnframes(), 800)
            await ws.close()
        self.assertEqual(self.blackbox_forms, [c[0] for c in cases])

    async def test_blackbox_empty_malformed_and_overlong_transcripts_fail(self):
        self.config["asr"] = {"provider": "blackbox", "model": "sensevoice", "language": "Chinese",
                              "url": str(self.server.make_url("/asr-blackbox"))}
        for response in ({"result": []}, {"result": [None]}, {"text": "字" * 4000}, {"error": "failed"}):
            self.blackbox_result = response
            ws = await self.connect()
            await self.upload(ws, 1, b"\x00\x00" * 800)
            self.assertEqual((await self.until_done(ws))[-1]["type"], "error")
            await ws.close()
        self.assertEqual(self.counter, 0)

    @unittest.skipUnless(shutil.which("ffmpeg"), "ffmpeg required for WAV conversion")
    async def test_blackbox_tts_preserves_typed_parameters_and_converts_audio(self):
        self.configure_speech()
        self.config["tts"].update({"provider": "blackbox", "model": "voxcpmtts",
                                   "parameters": {"prompt_text": "参考文本", "inference_timesteps": 10,
                                                  "streaming": True, "streaming_mode": True}})
        ws = await self.connect()
        await self.upload(ws, 1, b"\x00\x00" * 800, tts=True)
        pcm = bytearray()
        while True:
            message = await asyncio.wait_for(ws.receive(), 5)
            if message.type == WSMsgType.BINARY:
                pcm.extend(message.data[4:])
            elif json.loads(message.data)["type"] in ("done", "error"):
                self.assertEqual(json.loads(message.data)["type"], "done")
                break
        self.assertEqual(len(pcm), 3200)
        request = self.tts_requests[0]
        self.assertEqual(request["text"], "最终答复：麦克风测试")
        settings = request["settings"]
        self.assertEqual(settings["model_name"], "voxcpmtts")
        self.assertIs(settings["streaming"], False)
        self.assertIs(settings["streaming_mode"], False)
        self.assertEqual(settings["media_type"], "wav")
        self.assertEqual(settings["inference_timesteps"], 10)
        self.assertNotIn("voice", request)
        self.assertNotIn("input", request)
        await ws.close()

    def test_speech_provider_and_parameters_validated_on_startup(self):
        for invalid in ({"asr": {"provider": "typo"}}, {"tts": {"parameters": "not a table"}}):
            with self.assertRaises(ValueError):
                create_app(invalid)

    def configure_speech(self):
        self.config["asr"] = {"url": str(self.server.make_url("/asr")), "model": "test-asr"}
        self.config["tts"] = {"url": str(self.server.make_url("/tts")), "model": "test-tts", "voice": "test-voice"}
        os.environ["TTS_API_KEY"] = "test-tts-key"

    async def test_asr_auth_and_failure_never_send_audio_to_tts(self):
        self.configure_speech()
        self.config["asr"]["key_env"] = "TEST_ASR_API_KEY"
        os.environ["TEST_ASR_API_KEY"] = "test-asr-key"
        self.asr_status = 401
        ws = await self.connect()
        await self.upload(ws, 1, b"\x00\x00" * 800, tts=True)
        events = await self.until_done(ws)
        self.assertEqual(events[-1]["type"], "error")
        self.assertEqual(self.asr_auth, "Bearer test-asr-key")
        self.assertNotIn("test-asr-key", str(events))
        self.assertEqual(self.counter, 0)
        self.assertFalse(self.tts_requests)
        await ws.close()

    @unittest.skipUnless(shutil.which("ffmpeg"), "ffmpeg required for WAV conversion")
    async def test_voice_reply_converts_wav_and_sends_bounded_tagged_pcm(self):
        self.configure_speech()
        ws = await self.connect()
        await self.upload(ws, 31, b"\x01\x00" * 800, tts=True)
        events, pcm = [], bytearray()
        while True:
            message = await asyncio.wait_for(ws.receive(), 5)
            if message.type == WSMsgType.BINARY:
                self.assertLessEqual(len(message.data), 2052)
                self.assertEqual(struct.unpack_from("<I", message.data)[0], 31)
                pcm.extend(message.data[4:])
            else:
                event = json.loads(message.data)
                events.append(event)
                if event["type"] in ("done", "error"):
                    break
        self.assertEqual([e["type"] for e in events], ["heard", "sent", "reply", "audio", "audio_end", "done"])
        self.assertEqual(events[3]["frames"], 1600)
        self.assertEqual(len(pcm), 3200)
        self.assertTrue(all(abs(v[0] - 1234) <= 2 for v in struct.iter_unpack("<h", pcm)))
        self.assertEqual(self.tts_requests[0], {"model": "test-tts", "voice": "test-voice",
                        "input": "最终答复：麦克风测试", "response_format": "wav"})
        await ws.close()

    @unittest.skipUnless(shutil.which("ffmpeg"), "ffmpeg required")
    async def test_streaming_starts_tts_before_final_without_repeating_sentences(self):
        self.configure_speech()
        self.stream_parts = ["第一句话。", "第二句话。"]
        ws = await self.connect()
        await self.upload(ws, 1, b"\x00\x00" * 800, tts=True, streaming=True)
        events, audio = [], bytearray()
        while True:
            message = await asyncio.wait_for(ws.receive(), 5)
            if message.type == WSMsgType.BINARY:
                self.assertEqual(struct.unpack_from("<I", message.data)[0], 1)
                if not audio:
                    self.assertFalse(self.threads["1"][-1]["final"])
                    self.allow_final.set()
                audio.extend(message.data[4:])
            else:
                event = json.loads(message.data)
                events.append(event)
                if event["type"] in ("done", "error"):
                    break
        self.assertTrue(self.early_tts)
        self.assertEqual([r["input"] for r in self.tts_requests], self.stream_parts)
        self.assertEqual(len(audio), 6400)
        captions = [e for e in events if e["type"] == "caption"]
        self.assertEqual([e["text"] for e in captions], self.stream_parts)
        self.assertEqual([e["frame"] for e in captions], [0, 1600])
        self.assertEqual(sum(e["type"] == "audio" for e in events), 1)
        self.assertTrue(next(e for e in events if e["type"] == "audio")["stream"])
        self.assertEqual([e["type"] for e in events[-2:]], ["audio_end", "done"])
        await ws.close()

    @unittest.skipUnless(shutil.which("ffmpeg"), "ffmpeg required")
    async def test_voxcpm_pcm_arrives_before_tts_http_eof(self):
        self.configure_speech()
        self.config["tts"].update(provider="blackbox", model="voxcpmtts")
        self.stream_tts = True
        session = Session(None, self.client.server.app)
        chunks = session.synthesize_chunks("这是一小段测试语音")
        first = await asyncio.wait_for(anext(chunks), 3)
        self.assertTrue(first)
        self.assertFalse(self.allow_tts_end.is_set())
        self.allow_tts_end.set()
        pcm = first + b"".join([chunk async for chunk in chunks])
        self.assertEqual(len(pcm), 32000)
        self.assertTrue(all(abs(v[0] - 1234) <= 2 for v in struct.iter_unpack("<h", pcm)))

    @unittest.skipUnless(shutil.which("ffmpeg"), "ffmpeg required")
    async def test_voxcpm_cancellation_reaps_converter(self):
        self.configure_speech()
        self.config["tts"].update(provider="blackbox", model="voxcpmtts")
        self.stream_tts = True
        chunks = Session(None, self.client.server.app).synthesize_chunks("测试中断语音")
        self.assertTrue(await asyncio.wait_for(anext(chunks), 3))
        await asyncio.wait_for(chunks.aclose(), 1)
        self.allow_tts_end.set()

    @unittest.skipUnless(shutil.which("ffmpeg"), "ffmpeg required")
    async def test_voxcpm_rejected_stream_falls_back_before_any_audio(self):
        self.configure_speech()
        self.config["tts"].update(provider="blackbox", model="voxcpmtts")
        self.reject_stream = True
        chunks = Session(None, self.client.server.app).synthesize_chunks("测试回退")
        pcm = b"".join([chunk async for chunk in chunks])
        self.assertEqual(len(pcm), 3200)
        self.assertEqual([r["settings"]["streaming"] for r in self.tts_requests], [True, False])

    async def test_short_phrase_without_sentence_end_starts_tts(self):
        self.configure_speech()
        self.stream_parts = ["我们先来看看这个问题", "然后再继续回答。"]
        ws = await self.connect()
        await self.upload(ws, 1, b"\x00\x00" * 800, tts=True, streaming=True)
        await asyncio.wait_for(self.tts_started.wait(), 2)
        self.assertFalse(self.threads["1"][-1]["final"])
        self.assertEqual(self.tts_requests[0]["input"], self.stream_parts[0])
        self.allow_final.set()
        while True:
            message = await asyncio.wait_for(ws.receive(), 3)
            if message.type == WSMsgType.TEXT and json.loads(message.data)["type"] == "done":
                break
        self.assertEqual("".join(r["input"] for r in self.tts_requests), "".join(self.stream_parts))
        await ws.close()

    async def test_streaming_missing_live_endpoint_falls_back_to_final(self):
        self.configure_speech()
        self.disable_live = True
        ws = await self.connect()
        await self.upload(ws, 1, b"\x00\x00" * 800, tts=True, streaming=True)
        received = 0
        while True:
            message = await asyncio.wait_for(ws.receive(), 3)
            if message.type == WSMsgType.BINARY:
                received += len(message.data) - 4
            elif json.loads(message.data)["type"] == "done":
                break
        self.assertEqual(received, 3200)
        self.assertEqual(len(self.tts_requests), 1)
        await ws.close()

    async def test_streaming_tts_failure_finishes_with_text(self):
        self.configure_speech()
        self.tts_status = 500
        self.stream_parts = ["第一句话。", "第二句话。"]
        self.allow_final.set()
        ws = await self.connect()
        await self.upload(ws, 1, b"\x00\x00" * 800, tts=True, streaming=True)
        events = await self.until_done(ws)
        self.assertEqual(events[-1]["type"], "done")
        self.assertTrue(any(e["type"] == "warning" for e in events))
        self.assertTrue(any(e["type"] == "reply" and e["text"] == "第一句话。第二句话。" for e in events))
        self.assertFalse(any(e["type"] == "audio" for e in events))
        await ws.close()

    async def test_streaming_cancel_during_synthesis_stops_worker(self):
        self.configure_speech()
        self.stream_parts = ["第一句话。", "第二句话。"]
        self.tts_delay = 0.2
        ws = await self.connect()
        await self.upload(ws, 1, b"\x00\x00" * 800, tts=True, streaming=True)
        await asyncio.wait_for(self.tts_started.wait(), 2)
        await ws.send_json({"type": "cancel", "id": 1})
        # A new typed turn must not receive old PCM after cancellation.
        self.stream_parts = None
        await ws.send_json({"type": "text", "id": 2, "text": "new"})
        while True:
            message = await asyncio.wait_for(ws.receive(), 3)
            self.assertEqual(message.type, WSMsgType.TEXT)
            if json.loads(message.data).get("type") == "done":
                break
        await asyncio.sleep(0.25)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(ws.receive(), 0.03)
        await ws.close()

    async def test_older_firmware_and_usb_text_never_receive_binary_audio(self):
        self.configure_speech()
        ws = await self.connect()
        await self.upload(ws, 1, b"\x00\x00" * 800)
        self.assertEqual((await self.until_done(ws))[-1]["type"], "done")
        await ws.send_json({"type": "text", "id": 2, "text": "hello"})
        self.assertEqual((await self.until_done(ws))[-1]["type"], "done")
        self.assertEqual(self.tts_requests, [])
        await ws.close()

    async def test_tts_errors_keep_reply_and_finish_successfully(self):
        self.configure_speech()
        for status, invalid in [(401, False), (200, True)]:
            self.tts_status, self.tts_invalid = status, invalid
            ws = await self.connect()
            await self.upload(ws, 1, b"\x00\x00" * 800, tts=True)
            events = await self.until_done(ws)
            self.assertEqual([e["type"] for e in events], ["heard", "sent", "reply", "warning", "done"])
            self.assertNotIn("test-tts-key", str(events))
            await ws.close()

    @unittest.skipUnless(shutil.which("ffmpeg"), "ffmpeg required for WAV conversion")
    async def test_tts_duration_and_response_size_are_bounded(self):
        self.configure_speech()
        for bound in ("MAX_SPEECH", "MAX_TTS_WAV"):
            ws = await self.connect()
            with patch("gateway." + bound, 100):
                await self.upload(ws, 1, b"\x00\x00" * 800, tts=True)
                events = await self.until_done(ws)
            self.assertEqual(events[-2]["type"], "warning")
            self.assertEqual(events[-1]["type"], "done")
            await ws.close()

    async def test_cancel_during_tts_drops_old_audio(self):
        self.configure_speech()
        self.tts_delay = 0.2
        ws = await self.connect()
        await self.upload(ws, 1, b"\x00\x00" * 800, tts=True)
        for _ in range(3):
            await ws.receive_json()
        while not self.tts_requests:
            await asyncio.sleep(0.005)
        await ws.send_json({"type": "cancel", "id": 1})
        await ws.send_json({"type": "text", "id": 2, "text": "new"})
        events = await self.until_done(ws)
        self.assertTrue(all(e["id"] == 2 for e in events))
        self.assertEqual(self.counter, 2)
        await asyncio.sleep(0.25)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(ws.receive(), 0.03)
        await ws.close()

    async def test_cancel_stops_upstream_and_isolates_next_thread(self):
        self.final_after = 10000
        ws = await self.connect()
        await ws.send_json({"type": "text", "id": 1, "text": "slow"})
        self.assertEqual((await ws.receive_json())["type"], "sent")
        await ws.send_json({"type": "cancel", "id": 1})
        self.final_after = 2
        await ws.send_json({"type": "text", "id": 2, "text": "new"})
        events = await self.until_done(ws)
        self.assertTrue(all(e["id"] == 2 for e in events))
        self.assertEqual(events[1]["text"], "最终答复：new")
        self.assertEqual(self.stopped, ["1"])
        self.assertEqual(self.counter, 2)
        await ws.close()

    async def test_upstream_auth_error_does_not_expose_token(self):
        self.fail_send = True
        ws = await self.connect()
        await ws.send_json({"type": "text", "id": 1, "text": "hi"})
        events = await self.until_done(ws)
        self.assertEqual(events[-1]["type"], "error")
        self.assertNotIn("private-server-key", str(events))
        await ws.close()

    async def test_invalid_audio_size_and_format_are_bounded(self):
        ws = await self.connect()
        await ws.send_json({"type": "begin", "id": 1, "rate": 48000, "channels": 1, "format": "s16le"})
        self.assertEqual((await ws.receive_json())["type"], "error")
        await ws.send_json({"type": "begin", "id": 2, "rate": 16000, "channels": 1, "format": "s16le"})
        for _ in range(MAX_AUDIO // 2048 + 1):
            await ws.send_bytes(struct.pack("<I", 2) + b"\0" * 2048)
        event = await ws.receive_json()
        self.assertEqual(event["type"], "error")
        self.assertIn("16 秒", event["text"])
        await ws.close()

    async def test_upload_deadline_also_applies_when_packets_keep_arriving(self):
        ws = await self.connect()
        with patch("gateway.UPLOAD_TIMEOUT", 0.01):
            await ws.send_json({"type": "begin", "id": 1, "rate": 16000, "channels": 1, "format": "s16le"})
            await asyncio.sleep(0.03)
            await ws.send_bytes(struct.pack("<I", 1) + b"\0\0")
            event = await ws.receive_json()
            self.assertEqual(event["type"], "error")
            self.assertIn("上传超时", event["text"])
        await ws.close()

    async def test_timeout_stops_agent_and_returns_error(self):
        self.final_after = 10000
        self.client.server.app[NANO].timeout = 0.03
        ws = await self.connect()
        await ws.send_json({"type": "text", "id": 1, "text": "wait"})
        events = await self.until_done(ws)
        self.assertEqual(events[-1]["type"], "error")
        self.assertIn("超时", events[-1]["text"])
        await ws.close()

    async def test_json_escaping_cannot_exceed_firmware_receive_buffer(self):
        class Socket:
            async def send_str(self, text):
                self.wire = text
        socket = Socket()
        session = Session(socket, self.client.server.app)
        session.id = 42
        await session.send("reply", "\x01\"\\" * 2000)
        self.assertLessEqual(len(socket.wire.encode()), MAX_JSON)
        message = json.loads(socket.wire)
        self.assertEqual(message["id"], 42)
        self.assertIn("全文", message["text"])

    def test_utf8_reply_truncation_and_missing_key(self):
        reply = trim_utf8("你好🙂" * 3000)
        self.assertLessEqual(len(reply.encode()), 6000)
        self.assertIn("全文", reply)
        with patch.dict(os.environ, {"CORES3_DEVICE_KEY": ""}):
            with self.assertRaises(ValueError):
                create_app({})


if __name__ == "__main__":
    unittest.main()
