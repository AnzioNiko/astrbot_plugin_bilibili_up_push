from __future__ import annotations

import re
from typing import Literal


_NETWORK_ERROR_NAMES = {
    "networkerror",
    "timeouterror",
    "connectionerror",
    "connectionreseterror",
    "connectionabortederror",
    "connectionrefusederror",
    "brokenpipeerror",
}


def normalize_retry_times(value: object, default: int = 2, maximum: int = 10) -> int:
    """把配置值规范为 0 到 maximum 之间的重试次数。"""
    if isinstance(value, bool):
        return default
    try:
        retries = int(value)
    except (TypeError, ValueError):
        return default
    return min(max(retries, 0), maximum)


def format_exception_detail(exc: Exception) -> str:
    """生成不包含凭据和完整远程地址的错误摘要。"""
    http_message = _format_http_response_error(exc)
    if http_message:
        return http_message

    if _is_network_error(exc):
        detail = _sanitize_detail(exc) or "网络连接失败"
        return f"{type(exc).__name__}: {detail}"

    if isinstance(exc, OSError):
        code = getattr(exc, "winerror", None) or exc.errno
        if code is not None:
            code_label = (
                f"WinError {code}"
                if getattr(exc, "winerror", None)
                else f"错误码 {code}"
            )
            detail = exc.strerror or _sanitize_detail(exc) or "未提供错误详情"
            return f"{type(exc).__name__}（{code_label}）：{detail}"

    detail = _sanitize_detail(exc)
    return f"{type(exc).__name__}: {detail}" if detail else type(exc).__name__


def format_video_file_error(
    stage: Literal["parse", "send"],
    exc: Exception,
) -> str:
    """生成视频文件处理失败的用户提示。"""
    if _is_network_error(exc):
        if stage == "parse":
            reason = "解析或下载视频时网络连接失败，请检查网络、万能解析器和 B 站接口"
        else:
            reason = "机器人连接消息平台超时或网络异常，请检查 OneBot/NapCat 连接后重试"
    elif isinstance(exc, OSError):
        if stage == "parse":
            reason = "万能解析器返回的本地视频文件不可读"
        else:
            reason = "本地视频文件读取或上传失败"
    elif stage == "parse":
        reason = "万能解析器处理视频失败"
    else:
        reason = "机器人发送视频失败"

    return (
        f"视频文件未发送：{reason}。\n"
        f"原因：{format_exception_detail(exc)}"
    )


def _is_network_error(exc: Exception) -> bool:
    name = type(exc).__name__.lower()
    if name in _NETWORK_ERROR_NAMES:
        return True

    module = type(exc).__module__.lower()
    if module.startswith(("aiohttp.", "httpx.", "httpcore.", "aiocqhttp.")):
        return any(
            keyword in name
            for keyword in ("network", "connect", "timeout", "disconnect")
        )
    return False


def _format_http_response_error(exc: Exception) -> str | None:
    name = type(exc).__name__
    module = type(exc).__module__.lower()
    if not (
        name == "ClientResponseError"
        or (module.startswith("aiohttp.") and name.endswith("ClientResponseError"))
    ):
        return None

    status = getattr(exc, "status", None)
    if status is None:
        return None

    message = str(getattr(exc, "message", "") or "").strip()
    request_info = getattr(exc, "request_info", None)
    real_url = getattr(request_info, "real_url", None)
    host = str(getattr(real_url, "host", "") or "").strip()
    detail = f"HTTP {status} {message}".strip()
    return f"{detail}（主机：{host}）" if host else detail


def _sanitize_detail(exc: Exception) -> str:
    detail = str(exc).strip()
    detail = re.sub(r"https?://[^\s'\"<>]+", "[远程地址已隐藏]", detail)
    detail = re.sub(r"(?i)\b[A-Z]:\\[^\s'\"<>]+", "[本机路径已隐藏]", detail)
    detail = re.sub(
        r"(?i)\b(?:SESSDATA|bili_jct|access_token|refresh_token|token|w_rid|sign)"
        r"\s*[=:]\s*[^&\s,;]+",
        "[凭据已隐藏]",
        detail,
    )
    return detail[:500]
