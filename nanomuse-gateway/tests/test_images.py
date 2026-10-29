"""Bounded image decoding, workspace selection and credential confinement."""
import asyncio
import io
import os
from pathlib import Path
import struct
import sys
import unittest
from unittest.mock import patch

from aiohttp import ClientSession, web
from aiohttp.test_utils import TestServer
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from images import ImageBridge, display_size, image_paths, rgb565, workspace_path
from gateway import NanoMuse


def png(size=(4, 2), color="red"):
    out = io.BytesIO()
    Image.new("RGB", size, color).save(out, "PNG")
    return out.getvalue()


class ImageTests(unittest.TestCase):
    def test_fit_preserves_whole_image_and_rgb565_byte_order(self):
        data = rgb565(png(), (4, 4))
        self.assertEqual(data, b"\0" * 8 + b"\xf8\0" * 8 + b"\0" * 8)
        self.assertEqual(rgb565(png(color="lime"), (4, 2)), b"\x07\xe0" * 8)
        self.assertEqual(rgb565(png(color="blue"), (4, 2)), b"\0\x1f" * 8)

    def test_alpha_black_background_and_animation_first_frame(self):
        out = io.BytesIO()
        Image.new("RGBA", (4, 2), (255, 0, 0, 0)).save(out, "PNG")
        self.assertEqual(rgb565(out.getvalue(), (4, 2)), b"\0" * 16)
        out = io.BytesIO()
        Image.new("RGB", (4, 2), "red").save(out, "GIF", save_all=True,
            append_images=[Image.new("RGB", (4, 2), "blue")], duration=100, loop=0)
        self.assertEqual(rgb565(out.getvalue(), (4, 2)), b"\xf8\0" * 8)

    def test_jpeg_exif_orientation_and_webp(self):
        out = io.BytesIO()
        exif = Image.Exif()
        exif[274] = 6
        Image.new("RGB", (2, 4), "red").save(out, "JPEG", quality=100, exif=exif)
        self.assertEqual(rgb565(out.getvalue(), (4, 4)), b"\0" * 8 + b"\xf8\0" * 8 + b"\0" * 8)
        out = io.BytesIO()
        Image.new("RGB", (4, 2), "blue").save(out, "WEBP", lossless=True)
        self.assertEqual(rgb565(out.getvalue(), (4, 2)), b"\0\x1f" * 8)

    def test_reject_invalid_oversized_or_unsupported_images(self):
        for data in (b"bad data", png((4097, 1024)), b'<svg xmlns="http://www.w3.org/2000/svg"/>'):
            with self.assertRaises((ValueError, OSError)):
                rgb565(data, (4, 2))
        out = io.BytesIO()
        Image.new("RGB", (4, 2)).save(out, "BMP")
        with self.assertRaises(ValueError):
            rgb565(out.getvalue(), (4, 2))

    def test_workspace_paths_and_markdown_never_select_remote_or_escape(self):
        for path in ("../x.png", "/etc/x.png", "dir/../x.png", "dir//x.png", "./x.png",
                     "https://example.com/x.png", "file:x.png", "x\\y.png", "x.png?q=1", "x.png\0", "x.svg"):
            self.assertIsNone(workspace_path(path))
        self.assertEqual(workspace_path("目录/a%20b.PNG"), "目录/a%20b.PNG")
        answer = ('![本地](/api/files/%E5%9B%BE/a%20b.png?signature=ignored) '
                  '![链接](pictures/b.jpg "title") ![绝对](https://evil/x.png) '
                  '![逃逸](/api/files/%2e%2e/secret.png) ![畸形](http://[)')
        self.assertEqual(image_paths([
            {"type": "artifact", "path": "earlier.webp", "action": "write"},
            {"type": "artifact", "path": "deleted.png", "action": "delete"}], answer),
            ["earlier.webp", "图/a b.png", "pictures/b.jpg"])

    def test_only_bounded_integer_device_sizes(self):
        for size in (None, {}, {"width": True, "height": 240}, {"width": 320.0, "height": 240},
                     {"width": 641, "height": 1}, {"width": -1, "height": 1},
                     {"width": 640, "height": 640}):
            self.assertIsNone(display_size(size))
        self.assertEqual(display_size({"width": 320, "height": 240}), (320, 240))


class ImageHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.hit = []
        self.body = png()
        self.redirect = False
        self.chunked = False
        self.delay = False
        self.started = asyncio.Event()
        self.finish = asyncio.Event()
        async def file(request):
            self.hit.append((request.path, request.headers.get("Authorization")))
            self.started.set()
            if self.redirect:
                raise web.HTTPFound("/credential-leak")
            if self.delay:
                await self.finish.wait()
            if self.chunked:
                response = web.StreamResponse()
                await response.prepare(request)
                try:
                    for part in (self.body[:10], self.body[10:]):
                        await response.write(part)
                    await response.write_eof()
                except ConnectionResetError:
                    pass
                return response
            return web.Response(body=self.body)
        async def leak(request):
            self.fail("redirect must never receive credential")
        app = web.Application()
        app.router.add_get("/api/files/{path:.*}", file)
        app.router.add_get("/credential-leak", leak)
        self.server = TestServer(app)
        await self.server.start_server()
        self.http = ClientSession()
        with patch.dict(os.environ, {"NANOMUSE_SERVER_TOKEN": "secret-stays-on-PC"}):
            nano = NanoMuse(self.http, {"url": str(self.server.make_url(""))})
        self.bridge = ImageBridge(nano, {})

    async def asyncTearDown(self):
        self.finish.set()
        await self.http.close()
        await self.server.close()

    async def test_authenticated_bounded_stream_and_encoded_unicode_path(self):
        self.chunked = True
        self.assertEqual(await self.bridge.fetch("图/a%20b.png", (4, 2)), b"\xf8\0" * 8)
        self.assertEqual(self.hit, [("/api/files/图/a%20b.png", "Bearer secret-stays-on-PC")])

    async def test_no_redirect_or_arbitrary_url_even_with_valid_token(self):
        self.redirect = True
        with self.assertRaises(ValueError):
            await self.bridge.fetch("preview.png", (4, 2))
        with self.assertRaises(ValueError):
            await self.bridge.fetch("http://evil/leak.png", (4, 2))
        self.assertEqual(len(self.hit), 1)

    async def test_size_cap_with_and_without_content_length(self):
        self.body = b"x" * 8193
        with patch("images.MAX_SOURCE_BYTES", 8192):
            for chunked in (False, True):
                self.chunked = chunked
                with self.assertRaises(ValueError):
                    await self.bridge.fetch("too-large.png", (4, 2))

    async def test_disabled_preview_and_cancellation_do_not_finish_transfer(self):
        self.bridge.enabled = False
        with self.assertRaises(ValueError):
            await self.bridge.fetch("preview.png", (4, 2))
        self.assertEqual(self.hit, [])
        self.bridge.enabled = self.delay = True
        task = asyncio.create_task(self.bridge.fetch("preview.png", (4, 2)))
        await self.started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
