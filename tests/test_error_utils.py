from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

PLUGIN_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLUGIN_ROOT))

from error_utils import (
    format_exception_detail,
    format_video_file_error,
    normalize_retry_times,
)


class FakeClientResponseError(Exception):
    pass


FakeClientResponseError.__module__ = "aiohttp.client_exceptions"


class NetworkError(OSError):
    pass


NetworkError.__module__ = "aiocqhttp.exceptions"


class ErrorUtilsTest(unittest.TestCase):
    def test_retry_times_are_normalized_and_capped(self):
        self.assertEqual(normalize_retry_times(None), 2)
        self.assertEqual(normalize_retry_times("3"), 3)
        self.assertEqual(normalize_retry_times(-1), 0)
        self.assertEqual(normalize_retry_times(99), 10)
        self.assertEqual(normalize_retry_times("invalid"), 2)
        self.assertEqual(normalize_retry_times(True), 2)

    def test_network_error_is_not_reported_as_local_file_error(self):
        error = NetworkError("WebSocket API call timeout")

        detail = format_exception_detail(error)

        self.assertIn("NetworkError", detail)
        self.assertIn("WebSocket API call timeout", detail)
        self.assertNotIn("本地文件操作失败", detail)
        self.assertNotIn("错误码 None", detail)

    def test_send_network_error_uses_actionable_message(self):
        error = NetworkError("WebSocket API call timeout")

        message = format_video_file_error("send", error)

        self.assertIn("机器人连接消息平台超时或网络异常", message)
        self.assertIn("WebSocket API call timeout", message)

    def test_parser_error_uses_parser_specific_message(self):
        error = RuntimeError("解析失败")

        message = format_video_file_error("parse", error)

        self.assertIn("万能解析器处理视频失败", message)
        self.assertIn("RuntimeError", message)

    def test_real_file_error_keeps_operating_system_detail(self):
        error = FileNotFoundError(2, "系统找不到指定的文件。", "missing.mp4")

        detail = format_exception_detail(error)

        self.assertIn("FileNotFoundError", detail)
        self.assertIn("系统找不到指定的文件", detail)
        self.assertIn("2", detail)

    def test_http_error_hides_full_url_and_keeps_host(self):
        error = FakeClientResponseError("HTTP 502")
        error.status = 502
        error.message = "Bad Gateway"
        error.request_info = SimpleNamespace(
            real_url=SimpleNamespace(host="api.bilibili.com")
        )

        detail = format_exception_detail(error)

        self.assertIn("HTTP 502 Bad Gateway", detail)
        self.assertIn("api.bilibili.com", detail)
        self.assertNotIn("https://", detail)

    def test_sensitive_values_and_paths_are_sanitized(self):
        sensitive_cookie = "SESS" + "DATA=placeholder-value"
        error = RuntimeError(
            f"request https://example.com/video C:\\secret\\video.mp4 "
            f"{sensitive_cookie}"
        )

        message = format_video_file_error("parse", error)

        self.assertNotIn("https://example.com", message)
        self.assertNotIn(r"C:\secret\video.mp4", message)
        self.assertNotIn("placeholder-value", message)


if __name__ == "__main__":
    unittest.main()
