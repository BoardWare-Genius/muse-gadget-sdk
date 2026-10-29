"""Workspace image previews. Credentials and image decoding stay on the gateway."""
from __future__ import annotations

import asyncio
import io
import re
import struct
from urllib.parse import quote, unquote, urlsplit

from aiohttp import ClientTimeout
from PIL import Image, ImageOps, UnidentifiedImageError

MAX_SOURCE_BYTES = 8 * 1024 * 1024
MAX_SOURCE_PIXELS = 4_194_304
MAX_DISPLAY_PIXELS = 320 * 480
EXTENSIONS = (".png", ".jpg", ".jpeg", ".webp", ".gif")
# ID zero is reserved (normal PCM starts with a nonzero turn ID).
IMAGE_MAGIC = b"\0\0\0\0IMG1"


def workspace_path(value):
    if (not isinstance(value, str) or not value or len(value.encode()) > 1024
            or value.startswith("/") or "\\" in value or ":" in value
            or "?" in value or "#" in value or any(ord(c) < 32 for c in value)
            or any(p in ("", ".", "..") for p in value.split("/"))
            or not value.lower().endswith(EXTENSIONS)):
        return None
    return value


def image_paths(events, answer):
    """Only artifacts from this foreground turn and explicit workspace image links."""
    paths = []
    for event in events:
        if event.get("type") == "artifact" and event.get("action") in ("write", "update"):
            path = workspace_path(event.get("path"))
            if path and path not in paths:
                paths.append(path)
    for match in re.finditer(r'!\[[^\]]*\]\(\s*(?:<([^>]+)>|([^\s)]+))(?:\s+["\'][^)]*["\'])?\s*\)', answer):
        try:
            url = urlsplit(match[1] or match[2])
        except ValueError:
            continue
        if url.scheme or url.netloc:
            continue
        path = unquote(url.path)
        if path.startswith("/api/files/"):
            path = path[len("/api/files/"):]
        path = workspace_path(path)
        if path and path not in paths:
            paths.append(path)
    return paths[-8:]


def display_size(value):
    if not isinstance(value, dict):
        return None
    w, h = value.get("width"), value.get("height")
    if (type(w) is not int or type(h) is not int or not 1 <= w <= 640
            or not 1 <= h <= 640 or w * h > MAX_DISPLAY_PIXELS):
        return None
    return w, h


def rgb565(data, size):
    """Fit without cropping, orient EXIF, flatten alpha, return big-endian RGB565."""
    with Image.open(io.BytesIO(data)) as image:
        if image.format not in ("PNG", "JPEG", "WEBP", "GIF"):
            raise ValueError("unsupported image format")
        if image.width * image.height > MAX_SOURCE_PIXELS:
            raise ValueError("image dimensions too large")
        image.seek(0)
        rgba = ImageOps.exif_transpose(image).convert("RGBA")
        rgba.thumbnail(size, Image.Resampling.LANCZOS)
        canvas = Image.new("RGB", size, "black")
        canvas.paste(rgba, ((size[0] - rgba.width) // 2, (size[1] - rgba.height) // 2), rgba)
        output = bytearray(size[0] * size[1] * 2)
        source = canvas.tobytes()
        for n in range(size[0] * size[1]):
            r, g, b = source[n * 3:n * 3 + 3]
            struct.pack_into(">H", output, n * 2, (r & 248) << 8 | (g & 252) << 3 | b >> 3)
        return bytes(output)


class ImageBridge:
    def __init__(self, nano, config):
        self.nano = nano
        self.enabled = config.get("enabled", True)
        self.decode_lock = asyncio.Lock()

    async def fetch(self, path, size):
        if not self.enabled or not workspace_path(path) or not self.nano.token:
            raise ValueError("image preview unavailable")
        # Never fetch arbitrary model URLs or follow redirects with a server credential.
        async with self.nano.http.get(
                self.nano.base + "/api/files/" + quote(path, safe="/"),
                headers={"Authorization": "Bearer " + self.nano.token},
                allow_redirects=False, timeout=ClientTimeout(total=10)) as response:
            if response.status != 200:
                raise ValueError("image download failed")
            if response.content_length is not None and response.content_length > MAX_SOURCE_BYTES:
                raise ValueError("image file too large")
            data = bytearray()
            async for chunk in response.content.iter_chunked(65536):
                if len(data) + len(chunk) > MAX_SOURCE_BYTES:
                    raise ValueError("image file too large")
                data.extend(chunk)
        # Keep one decoder running, even if a turn is cancelled during decoding.
        async with self.decode_lock:
            task = asyncio.create_task(asyncio.to_thread(rgb565, bytes(data), size))
            try:
                return await asyncio.shield(task)
            except Image.DecompressionBombError as exc:
                raise ValueError("image dimensions too large") from exc
            except asyncio.CancelledError:
                try:
                    await task
                except (ValueError, OSError, Image.DecompressionBombError, UnidentifiedImageError):
                    pass
                raise
