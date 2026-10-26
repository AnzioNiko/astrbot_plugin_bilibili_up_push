from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path

PLUGIN_ROOT = Path(__file__).resolve().parents[1]


def install_import_stubs() -> None:
    astrbot = types.ModuleType("astrbot")
    astrbot.__path__ = []
    api = types.ModuleType("astrbot.api")
    api.logger = types.SimpleNamespace(
        info=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
        error=lambda *args, **kwargs: None,
    )

    event = types.ModuleType("astrbot.api.event")
    event.AstrMessageEvent = object

    class Filter:
        @staticmethod
        def command(*args, **kwargs):
            return lambda function: function

    event.filter = Filter()

    message_components = types.ModuleType("astrbot.api.message_components")

    class Plain:
        def __init__(self, text):
            self.text = text

    class Image:
        @staticmethod
        def fromFileSystem(path):
            return Path(path)

    message_components.Image = Image
    message_components.Plain = Plain

    star = types.ModuleType("astrbot.api.star")
    star.Context = object
    star.Star = object
    star.StarTools = types.SimpleNamespace(get_data_dir=lambda plugin_id: PLUGIN_ROOT)

    core = types.ModuleType("astrbot.core")
    core.__path__ = []
    core_message = types.ModuleType("astrbot.core.message")
    core_message.__path__ = []
    core_components = types.ModuleType("astrbot.core.message.components")

    class Video:
        @staticmethod
        def fromFileSystem(path):
            return Path(path)

    core_components.Video = Video
    message_result = types.ModuleType("astrbot.core.message.message_event_result")

    class MessageChain:
        def __init__(self, items):
            self.items = items

    message_result.MessageChain = MessageChain

    aiohttp = types.ModuleType("aiohttp")
    aiohttp.ClientTimeout = object
    aiohttp.ClientSession = object

    sys.modules.update(
        {
            "astrbot": astrbot,
            "astrbot.api": api,
            "astrbot.api.event": event,
            "astrbot.api.message_components": message_components,
            "astrbot.api.star": star,
            "astrbot.core": core,
            "astrbot.core.message": core_message,
            "astrbot.core.message.components": core_components,
            "astrbot.core.message.message_event_result": message_result,
            "aiohttp": aiohttp,
        }
    )


def load_plugin_module():
    package_name = "astrbot_plugin_bilibili_up_push_test"
    package = types.ModuleType(package_name)
    package.__path__ = [str(PLUGIN_ROOT)]
    sys.modules[package_name] = package

    spec = importlib.util.spec_from_file_location(
        f"{package_name}.main",
        PLUGIN_ROOT / "main.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


install_import_stubs()
plugin_module = load_plugin_module()


class NetworkError(OSError):
    pass


class FakeContext:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    async def send_message(self, origin, chain):
        self.calls.append((origin, chain))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class MainBehaviorTest(unittest.IsolatedAsyncioTestCase):
    def make_plugin(self, context, retry_times):
        plugin = object.__new__(plugin_module.BiliUpPushPlugin)
        plugin.context = context
        plugin.push_retry_times = retry_times
        plugin.send_video_file = True
        return plugin

    async def test_send_retries_network_error_until_success(self):
        context = FakeContext(
            [
                NetworkError("WebSocket API call timeout"),
                NetworkError("WebSocket API call timeout"),
                True,
            ]
        )
        plugin = self.make_plugin(context, retry_times=2)
        original_interval = plugin_module.PUSH_RETRY_INTERVAL_SECONDS
        plugin_module.PUSH_RETRY_INTERVAL_SECONDS = 0
        try:
            sent = await plugin._send_message_with_retry(
                "group",
                lambda: ["测试消息"],
                "测试消息",
            )
        finally:
            plugin_module.PUSH_RETRY_INTERVAL_SECONDS = original_interval

        self.assertTrue(sent)
        self.assertEqual(len(context.calls), 3)

    async def test_video_error_does_not_send_extra_chat_message(self):
        context = FakeContext([True])
        plugin = self.make_plugin(context, retry_times=0)

        async def fail_to_get_video(bvid):
            return None

        plugin._get_video_path_with_retry = fail_to_get_video
        text_sent, video_sent = await plugin._send_notification_messages(
            "group",
            "测试UP",
            {
                "bvid": "BV1TestVideo",
                "title": "测试视频",
                "description": "",
                "pubdate": "0",
            },
        )

        self.assertTrue(text_sent)
        self.assertFalse(video_sent)
        self.assertEqual(len(context.calls), 1)

    async def test_exhausted_text_retries_do_not_send_error_message(self):
        context = FakeContext([False, False, False])
        plugin = self.make_plugin(context, retry_times=2)
        original_interval = plugin_module.PUSH_RETRY_INTERVAL_SECONDS
        plugin_module.PUSH_RETRY_INTERVAL_SECONDS = 0
        try:
            text_sent, video_sent = await plugin._send_notification_messages(
                "group",
                "测试UP",
                {
                    "bvid": "BV1TestVideo",
                    "title": "测试视频",
                    "description": "",
                    "pubdate": "0",
                },
            )
        finally:
            plugin_module.PUSH_RETRY_INTERVAL_SECONDS = original_interval

        self.assertFalse(text_sent)
        self.assertFalse(video_sent)
        self.assertEqual(len(context.calls), 3)


if __name__ == "__main__":
    unittest.main()
