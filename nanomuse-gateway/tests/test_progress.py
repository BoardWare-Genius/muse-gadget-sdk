"""Virtual-clock tests for idle renewal, rate limits and absolute bounds."""
import asyncio
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from gateway import ReplyProgress


class ProgressTests(unittest.IsolatedAsyncioTestCase):
    async def test_virtual_long_answer_and_absolute_cap(self):
        clock = SimpleNamespace(now=0)
        with patch('gateway.time', SimpleNamespace(monotonic=lambda: clock.now)):
            progress = ReplyProgress(150, 900)
            sent = []
            async def send(kind):sent.append(kind)
            for t in range(0, 900, 100):
                clock.now = t
                progress.touch()
                self.assertFalse(progress.expired())
                await progress.report(send)
            clock.now = 900
            progress.touch()
            self.assertTrue(progress.expired())
            self.assertEqual(sent, ['progress'] * 8)

    async def test_polling_and_timestamp_changes_cannot_refresh_idle(self):
        clock = SimpleNamespace(now=0)
        with patch('gateway.time', SimpleNamespace(monotonic=lambda: clock.now)):
            progress = ReplyProgress(150, 900)
            event = {'id':'a', 'type':'tool', 'status':'running', 'ts':'first'}
            progress.observe([event])
            clock.now = 149
            event['ts'] = 'new timestamp'
            progress.observe([event])
            self.assertFalse(progress.expired())
            clock.now = 150
            self.assertTrue(progress.expired())

    async def test_only_changed_output_renews_and_reports_are_rate_limited(self):
        clock = SimpleNamespace(now=0)
        with patch('gateway.time', SimpleNamespace(monotonic=lambda: clock.now)):
            progress = ReplyProgress(150, 900)
            sent = []
            async def send(kind):sent.append(kind)
            event = {'id':'a','type':'assistant','text':'first','final':False}
            for t in (0, .5, 1, 2):
                clock.now = t
                progress.observe([event])
                await progress.report(send)
            self.assertEqual(sent, ['progress'])
            clock.now = 149
            event['text'] += ' new text'
            progress.observe([event])
            clock.now = 200
            self.assertFalse(progress.expired())
            clock.now = 299
            self.assertTrue(progress.expired())
