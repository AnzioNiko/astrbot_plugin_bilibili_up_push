"""按群订阅 B 站 UP 主的新视频，并可直接下载视频文件推送。"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import html
import json
import os
import re
import tempfile
import time
import urllib.parse
import uuid
from pathlib import Path
from typing import Any

import aiohttp
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import Image, Plain
from astrbot.core.message.components import Video
from astrbot.api.star import Context, Star, StarTools
from astrbot.core.message.message_event_result import MessageChain


PLUGIN_ID = "astrbot_plugin_bilibili_up_push"
PARSER_PLUGIN_ID = "astrbot_plugin_parser"
API_BASE = "https://api.bilibili.com"
PASSPORT_BASE = "https://passport.bilibili.com"
NAV_API = f"{API_BASE}/x/web-interface/nav"
USER_API = f"{API_BASE}/x/space/wbi/acc/info"
VIDEO_API = f"{API_BASE}/x/space/wbi/arc/search"
SEARCH_API = f"{API_BASE}/x/web-interface/wbi/search/type"
SEARCH_API_LEGACY = f"{API_BASE}/x/web-interface/search/type"
QR_GENERATE_API = f"{PASSPORT_BASE}/x/passport-login/web/qrcode/generate"
QR_POLL_API = f"{PASSPORT_BASE}/x/passport-login/web/qrcode/poll"
POLL_INTERVAL_SECONDS = 900
VIDEO_BATCH_SIZE = 10
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
MIXIN_TABLE = (
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35,
    27, 43, 5, 49, 33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13,
    37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4,
    22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52,
)


class BiliUpPushPlugin(Star):
    """将 UP 主更新通知发送到订阅所在的群。"""

    def __init__(self, context: Context, config: dict[str, Any] | None = None) -> None:
        super().__init__(context)
        self.context = context
        self.config = config or {}
        self.send_video_file = bool(self.config.get("send_video_file", True))
        self.data_dir = Path(StarTools.get_data_dir(PLUGIN_ID))
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = self.data_dir / "subscriptions.json"
        self.cookie_path = self.data_dir / "bili_cookies.json"
        self.subscriptions = self._load_state()
        self.cookies = self._load_cookies()
        self.buvid3 = self.cookies.get("buvid3") or f"{uuid.uuid4()}infoc"
        self.state_lock = asyncio.Lock()
        self.wbi_lock = asyncio.Lock()
        self.login_lock = asyncio.Lock()
        self.mixin_key: str | None = None
        self.mixin_key_time = 0.0
        self.session: aiohttp.ClientSession | None = None
        self.poll_task: asyncio.Task[None] | None = None

    async def initialize(self) -> None:
        timeout = aiohttp.ClientTimeout(total=20)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            headers=self._request_headers(),
        )
        if self.poll_task is None or self.poll_task.done():
            self.poll_task = asyncio.create_task(self._poll_loop())
        logger.info("[BiliUPPush] 已启动，轮询间隔 15 分钟")

    async def terminate(self) -> None:
        if self.poll_task and not self.poll_task.done():
            self.poll_task.cancel()
            try:
                await self.poll_task
            except asyncio.CancelledError:
                pass
        if self.session and not self.session.closed:
            await self.session.close()
        logger.info("[BiliUPPush] 已停止")

    @filter.command("订阅UP")
    async def subscribe_up(self, event: AstrMessageEvent):
        """订阅指定 UP 主，在本群接收新视频通知。"""
        event.stop_event()
        if not event.get_group_id():
            yield event.plain_result("请在要接收通知的群里使用此命令。")
            return
        if not self.is_logged_in():
            yield event.plain_result("请先私聊机器人使用 /B站登录，再添加 UP 主订阅。")
            return
        query = self._command_argument(event)
        if not query:
            yield event.plain_result("用法：/订阅UP <UP主用户名或UID>")
            return
        name = ""
        if query.isdigit():
            mid = query
        else:
            try:
                mid, name = await self._resolve_uploader_name(query)
            except LookupError as exc:
                yield event.plain_result(str(exc))
                return
            except Exception as exc:
                logger.warning(f"[BiliUPPush] 订阅搜索失败 query={query}: {exc}")
                yield event.plain_result("搜索 UP 主失败，请稍后重试或直接使用 UID。")
                return
        origin = event.unified_msg_origin
        try:
            latest = await self._latest_videos(mid, count=1)
        except Exception as exc:
            logger.warning(f"[BiliUPPush] 订阅校验失败 UID={mid}: {exc}")
            yield event.plain_result("获取 UP 主视频信息失败，请检查 UID 或稍后重试。")
            return
        if not name:
            try:
                name = await self._uploader_name(mid)
            except Exception as exc:
                logger.warning(f"[BiliUPPush] 获取 UP 主昵称失败 UID={mid}: {exc}")
                name = f"UP主_{mid}"

        entry = {
            "name": name or f"UP主_{mid}",
            "last_bvid": latest[0]["bvid"] if latest else "",
        }
        duplicate = False
        save_error: OSError | None = None
        async with self.state_lock:
            group_subs = self.subscriptions.setdefault(origin, {})
            if mid in group_subs:
                duplicate = True
                entry = group_subs[mid]
            else:
                group_subs[mid] = entry
                try:
                    self._save_state()
                except OSError as exc:
                    group_subs.pop(mid, None)
                    if not group_subs:
                        self.subscriptions.pop(origin, None)
                    save_error = exc
        if duplicate:
            yield event.plain_result(f"群里已经订阅了【{entry['name']}】(UID:{mid})。")
        elif save_error:
            logger.error(f"[BiliUPPush] 保存订阅失败: {save_error}")
            yield event.plain_result("保存订阅失败，请检查插件数据目录权限。")
        else:
            yield event.plain_result(
                f"已订阅【{entry['name']}】(UID:{mid})。之后有新视频时会在本群发送标题、简介和链接。"
            )

    @filter.command("取消订阅UP")
    async def unsubscribe_up(self, event: AstrMessageEvent):
        """取消本群对指定 UP 主的更新订阅。"""
        event.stop_event()
        if not event.get_group_id():
            yield event.plain_result("请在要管理订阅的群里使用此命令。")
            return
        query = self._command_argument(event)
        if not query:
            yield event.plain_result("用法：/取消订阅UP <UP主用户名或UID>")
            return
        origin = event.unified_msg_origin
        mid = query
        if not query.isdigit():
            async with self.state_lock:
                group_subs = self.subscriptions.get(origin, {})
                matches = [
                    subscribed_mid
                    for subscribed_mid, item in group_subs.items()
                    if self._clean_text(str(item.get("name", ""))) == query
                ]
            if not matches:
                yield event.plain_result(f"本群没有订阅昵称完全匹配【{query}】的 UP 主。")
                return
            if len(matches) > 1:
                yield event.plain_result(
                    f"本群有多个昵称为【{query}】的订阅，请使用 UID 取消：{', '.join(matches)}"
                )
                return
            mid = matches[0]
        removed: dict[str, str] | None = None
        save_error: OSError | None = None
        async with self.state_lock:
            group_subs = self.subscriptions.get(origin, {})
            removed = group_subs.pop(mid, None)
            if removed is not None:
                if not group_subs:
                    self.subscriptions.pop(origin, None)
                try:
                    self._save_state()
                except OSError as exc:
                    self.subscriptions.setdefault(origin, {})[mid] = removed
                    save_error = exc
        if removed is None:
            yield event.plain_result(f"本群没有订阅 UID:{mid}。")
        elif save_error:
            logger.error(f"[BiliUPPush] 保存取消订阅失败: {save_error}")
            yield event.plain_result("保存失败，订阅状态未能更新。")
        else:
            yield event.plain_result(f"已取消订阅【{removed['name']}】(UID:{mid})。")

    @filter.command("UP订阅列表")
    async def list_subscriptions(self, event: AstrMessageEvent):
        """查看当前群的 UP 主订阅列表。"""
        event.stop_event()
        if not event.get_group_id():
            yield event.plain_result("请在群里查看该群的订阅列表。")
            return
        async with self.state_lock:
            entries = copy.deepcopy(self.subscriptions.get(event.unified_msg_origin, {}))
        if not entries:
            yield event.plain_result("本群还没有订阅 UP 主。使用 /订阅UP <用户名或UID> 添加。")
            return
        lines = ["本群的 UP 主订阅："]
        lines.extend(f"• {item['name']} (UID:{mid})" for mid, item in entries.items())
        yield event.plain_result("\n".join(lines))

    @filter.command("测试UP推送")
    async def test_push(self, event: AstrMessageEvent):
        """向当前群即时发送指定 UP 主的最新视频通知，用于测试推送链路。"""
        event.stop_event()
        if not event.get_group_id():
            yield event.plain_result("请在要测试的群里使用此命令。")
            return
        if not self.is_logged_in():
            yield event.plain_result("请先私聊机器人使用 /B站登录，再测试推送。")
            return
        query = self._command_argument(event)
        if not query:
            yield event.plain_result("用法：/测试UP推送 <UP主用户名或UID>")
            return

        name = ""
        if query.isdigit():
            mid = query
        else:
            try:
                mid, name = await self._resolve_uploader_name(query)
            except LookupError as exc:
                yield event.plain_result(str(exc))
                return
            except Exception as exc:
                logger.warning(f"[BiliUPPush] 测试推送搜索失败 query={query}: {exc}")
                yield event.plain_result("搜索 UP 主失败，请稍后重试或直接使用 UID。")
                return

        try:
            videos = await self._latest_videos(mid, count=1)
            if not videos:
                yield event.plain_result(f"没有获取到 UID:{mid} 的视频，无法发送测试消息。")
                return
            if not name:
                try:
                    name = await self._uploader_name(mid)
                except Exception:
                    name = f"UP主_{mid}"
            display_name = name or f"UP主_{mid}"
            sent, _ = await self._send_notification_messages(
                event.unified_msg_origin, display_name, videos[0]
            )
        except Exception as exc:
            logger.warning(f"[BiliUPPush] 测试推送失败 UID={mid}: {exc}")
            yield event.plain_result("测试推送失败，请查看插件日志。")
            return
        if sent is False:
            yield event.plain_result("发送接口返回失败，请检查机器人是否仍在群内。")
            return
        # 成功时不再追加命令回复，避免视频文件之后又出现一条文本消息。
        return

    @filter.command("UP推送帮助")
    async def help(self, event: AstrMessageEvent):
        """查看订阅、登录和更新通知指令说明。"""
        event.stop_event()
        yield event.plain_result(
            "B 站 UP 主更新通知\n"
            "/B站登录：私聊机器人后扫码登录 B 站\n"
            "/B站登录状态：查看当前登录状态\n"
            "/B站登出：退出 B 站登录\n"
            "/订阅UP <用户名或UID>：按完全匹配的昵称或 UID 订阅；昵称重名时请用 UID\n"
            "/取消订阅UP <用户名或UID>：按本群记录的昵称或 UID 取消订阅\n"
            "/UP订阅列表：查看本群订阅\n"
            "/测试UP推送 <用户名或UID>：向本群即时发送最新视频，不修改订阅记录\n"
            "每 15 分钟检查一次；视频文件推送可在本插件设置中开关，清晰度、下载限制和解析器的 B 站登录态请在万能解析器中配置。"
        )

    @filter.command("B站登录")
    async def bili_login(self, event: AstrMessageEvent):
        """生成 B 站扫码登录二维码，并保存登录态供订阅请求使用。"""
        event.stop_event()
        if not event.is_admin():
            yield event.plain_result("只有机器人管理员可以设置 B 站登录。")
            return
        if event.get_group_id():
            yield event.plain_result("为保护登录二维码，请私聊机器人使用此指令。")
            return
        async with self.login_lock:
            if self.is_logged_in():
                yield event.plain_result("B 站已登录；如需更换账号，请先使用 /B站登出。")
                return

            yield event.plain_result("正在生成 B 站登录二维码……")
            qr_path: Path | None = None
            timeout = aiohttp.ClientTimeout(total=15)
            headers = {
                "User-Agent": USER_AGENT,
                "Referer": "https://www.bilibili.com/",
            }
            try:
                async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                    async with session.get(QR_GENERATE_API) as response:
                        response.raise_for_status()
                        payload = await response.json(content_type=None)
                    if payload.get("code") != 0:
                        yield event.plain_result(
                            f"生成二维码失败：{payload.get('message', 'B 站接口返回异常')}"
                        )
                        return

                    qr_data = payload.get("data") or {}
                    qr_url = str(qr_data.get("url", ""))
                    qr_key = str(qr_data.get("qrcode_key", ""))
                    if not qr_url or not qr_key:
                        yield event.plain_result("生成二维码失败：B 站未返回有效二维码信息。")
                        return

                    try:
                        import segno

                        qr_path = self.data_dir / f"login_qr_{uuid.uuid4().hex[:8]}.png"
                        segno.make(qr_url).save(str(qr_path), scale=8, border=4)
                    except ImportError:
                        yield event.plain_result("缺少二维码依赖 segno，请重载插件安装依赖后重试。")
                        return
                    except Exception as exc:
                        logger.warning(f"[BiliUPPush] 生成登录二维码图片失败: {exc}")
                        yield event.plain_result("生成二维码图片失败，请稍后重试。")
                        return

                    yield event.chain_result(
                        [
                            Plain("请使用 B 站 App 扫描二维码登录，二维码有效期约 3 分钟。\n"),
                            Image.fromFileSystem(str(qr_path)),
                        ]
                    )

                    deadline = asyncio.get_running_loop().time() + 180
                    scanned_notified = False
                    while asyncio.get_running_loop().time() < deadline:
                        async with session.get(
                            QR_POLL_API,
                            params={"qrcode_key": qr_key},
                        ) as response:
                            response.raise_for_status()
                            poll_payload = await response.json(content_type=None)
                            response_cookies = {
                                cookie.key: cookie.value
                                for cookie in response.cookies.values()
                                if cookie.value
                            }

                        poll_data = poll_payload.get("data") or {}
                        poll_code = poll_data.get("code")
                        if poll_code == 0:
                            login_url = str(poll_data.get("url", ""))
                            parsed_cookies = dict(
                                urllib.parse.parse_qsl(
                                    urllib.parse.urlsplit(login_url).query,
                                    keep_blank_values=False,
                                )
                            )
                            allowed_names = {
                                "SESSDATA", "bili_jct", "DedeUserID",
                                "DedeUserID__ckMd5", "sid", "buvid3",
                            }
                            new_cookies = {
                                key: value
                                for key, value in {**parsed_cookies, **response_cookies}.items()
                                if key in allowed_names and value
                            }
                            if not new_cookies.get("SESSDATA"):
                                yield event.plain_result("登录完成，但 B 站没有返回有效登录凭据。")
                                return
                            new_cookies.setdefault("buvid3", self.buvid3)
                            try:
                                self._save_cookies(new_cookies)
                            except OSError as exc:
                                logger.error(f"[BiliUPPush] 保存 B 站登录态失败: {exc}")
                                yield event.plain_result("登录成功，但保存登录态失败，请检查插件数据目录权限。")
                                return
                            self.cookies = new_cookies
                            self.buvid3 = new_cookies["buvid3"]
                            self._refresh_session_cookies()
                            yield event.plain_result("B 站扫码登录成功，现在可以订阅 UP 主了。")
                            return
                        if poll_code == 86038:
                            yield event.plain_result("登录二维码已过期，请重新使用 /B站登录。")
                            return
                        if poll_code == 86090 and not scanned_notified:
                            scanned_notified = True
                            yield event.plain_result("二维码已扫描，请在 B 站 App 确认登录。")
                        elif poll_code not in (86101, 86090):
                            logger.warning(
                                f"[BiliUPPush] B 站扫码轮询返回异常 code={poll_code}"
                            )
                            yield event.plain_result("B 站扫码登录失败，请重新使用 /B站登录。")
                            return
                        await asyncio.sleep(2)

                    yield event.plain_result("等待扫码登录超时，请重新使用 /B站登录。")
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                logger.warning(
                    f"[BiliUPPush] B 站扫码登录请求失败，异常类型={type(exc).__name__}"
                )
                yield event.plain_result("连接 B 站登录接口失败，请稍后重试。")
            finally:
                if qr_path and qr_path.exists():
                    try:
                        qr_path.unlink()
                    except OSError:
                        pass

    @filter.command("B站登录状态")
    async def bili_login_status(self, event: AstrMessageEvent):
        """查看 UP 主更新推送插件的 B 站登录状态。"""
        event.stop_event()
        yield event.plain_result(
            "当前已登录 B 站。" if self.is_logged_in() else "当前未登录 B 站，请私聊使用 /B站登录。"
        )

    @filter.command("B站登出")
    async def bili_logout(self, event: AstrMessageEvent):
        """清除本插件保存的 B 站登录态。"""
        event.stop_event()
        if not event.is_admin():
            yield event.plain_result("只有机器人管理员可以退出 B 站登录。")
            return
        if event.get_group_id():
            yield event.plain_result("为保护账号状态，请私聊机器人使用此指令。")
            return
        if not self.is_logged_in():
            yield event.plain_result("当前未登录 B 站。")
            return
        try:
            if self.cookie_path.exists():
                self.cookie_path.unlink()
        except OSError as exc:
            logger.error(f"[BiliUPPush] 删除 B 站登录态失败: {exc}")
            yield event.plain_result("退出登录失败，请检查插件数据目录权限。")
            return
        self.cookies = {}
        self._refresh_session_cookies()
        yield event.plain_result("已退出本插件的 B 站登录。")

    @staticmethod
    def _command_argument(event: AstrMessageEvent) -> str:
        message = (getattr(event, "message_str", "") or "").strip()
        parts = message.split(maxsplit=1)
        return parts[1].strip() if len(parts) == 2 else ""

    def _load_state(self) -> dict[str, dict[str, dict[str, str]]]:
        if not self.state_path.exists():
            return {}
        try:
            raw = json.loads(self.state_path.read_text(encoding="utf-8"))
            subscriptions = raw.get("subscriptions", {}) if isinstance(raw, dict) else {}
            return subscriptions if isinstance(subscriptions, dict) else {}
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning(f"[BiliUPPush] 读取订阅数据失败，将使用空列表: {exc}")
            return {}

    def _load_cookies(self) -> dict[str, str]:
        if not self.cookie_path.exists():
            return {}
        try:
            raw = json.loads(self.cookie_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning(f"[BiliUPPush] 读取 B 站登录态失败: {exc}")
            return {}
        if not isinstance(raw, dict) or not raw.get("SESSDATA"):
            return {}
        allowed_names = {
            "SESSDATA", "bili_jct", "DedeUserID",
            "DedeUserID__ckMd5", "sid", "buvid3",
        }
        return {
            str(key): str(value)
            for key, value in raw.items()
            if key in allowed_names and value
        }

    def _save_cookies(self, cookies: dict[str, str]) -> None:
        payload = json.dumps(cookies, ensure_ascii=False, indent=2)
        temp_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                "w", encoding="utf-8", delete=False,
                dir=self.data_dir, prefix="bili-cookies-", suffix=".tmp",
            ) as temp_file:
                temp_file.write(payload)
                temp_file.flush()
                os.fsync(temp_file.fileno())
                temp_path = temp_file.name
            os.replace(temp_path, self.cookie_path)
            try:
                os.chmod(self.cookie_path, 0o600)
            except OSError:
                pass
        finally:
            if temp_path and os.path.exists(temp_path):
                os.unlink(temp_path)

    def is_logged_in(self) -> bool:
        return bool(self.cookies.get("SESSDATA"))

    def _request_headers(self) -> dict[str, str]:
        headers = {
            "User-Agent": USER_AGENT,
            "Referer": "https://www.bilibili.com/",
        }
        cookies = dict(self.cookies)
        cookies.setdefault("buvid3", self.buvid3)
        headers["Cookie"] = "; ".join(
            f"{key}={value}" for key, value in cookies.items() if value
        )
        return headers

    def _refresh_session_cookies(self) -> None:
        if self.session is None or self.session.closed:
            return
        self.session.cookie_jar.clear()
        self.session.headers.pop("Cookie", None)
        self.session.headers.update(self._request_headers())

    def _save_state(self) -> None:
        payload = json.dumps(
            {"subscriptions": self.subscriptions}, ensure_ascii=False, indent=2
        )
        temp_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                "w", encoding="utf-8", delete=False,
                dir=self.data_dir, prefix="subscriptions-", suffix=".tmp",
            ) as temp_file:
                temp_file.write(payload)
                temp_file.flush()
                os.fsync(temp_file.fileno())
                temp_path = temp_file.name
            os.replace(temp_path, self.state_path)
        finally:
            if temp_path and os.path.exists(temp_path):
                os.unlink(temp_path)

    async def _poll_loop(self) -> None:
        await asyncio.sleep(15)
        while True:
            try:
                await self._check_subscriptions()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(f"[BiliUPPush] 轮询异常: {exc}")
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

    async def _check_subscriptions(self) -> None:
        if not self.is_logged_in():
            return
        async with self.state_lock:
            snapshot = copy.deepcopy(self.subscriptions)
        for origin, group_subs in snapshot.items():
            for mid, item in group_subs.items():
                try:
                    videos = await self._latest_videos(mid, count=VIDEO_BATCH_SIZE)
                    if not videos:
                        continue
                    old_bvid = item.get("last_bvid", "")
                    ids = [video["bvid"] for video in videos]
                    if not old_bvid:
                        await self._set_last_bvid(origin, mid, ids[0])
                        continue
                    if old_bvid in ids:
                        index = ids.index(old_bvid)
                        new_videos = list(reversed(videos[:index]))
                    elif ids[0] != old_bvid:
                        # 旧记录超出最近批次时只补最新一条，避免重启后批量刷屏。
                        new_videos = [videos[0]]
                    else:
                        new_videos = []
                    for video in new_videos:
                        current = await self._get_subscription(origin, mid)
                        if current is None:
                            break
                        sent, video_attached = await self._send_notification_messages(
                            origin, current["name"], video
                        )
                        if sent is False:
                            logger.warning(
                                f"[BiliUPPush] 视频信息发送失败，保留待重试: UID={mid} BV={video['bvid']}"
                            )
                            break
                        await self._set_last_bvid(origin, mid, video["bvid"])
                        logger.info(
                            f"[BiliUPPush] 已推送 UID={mid} BV={video['bvid']} 到 {origin}，"
                            f"视频附件={'已发送' if video_attached else '未发送'}"
                        )
                        await asyncio.sleep(0.5)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(f"[BiliUPPush] 检查 UID={mid} 失败: {exc}")
                await asyncio.sleep(0.3)

    async def _get_subscription(self, origin: str, mid: str) -> dict[str, str] | None:
        async with self.state_lock:
            item = self.subscriptions.get(origin, {}).get(mid)
            return copy.deepcopy(item) if item else None

    async def _set_last_bvid(self, origin: str, mid: str, bvid: str) -> None:
        async with self.state_lock:
            item = self.subscriptions.get(origin, {}).get(mid)
            if item is None:
                return
            previous = item.get("last_bvid", "")
            item["last_bvid"] = bvid
            try:
                self._save_state()
            except OSError:
                item["last_bvid"] = previous
                raise

    async def _api_json(
        self,
        url: str,
        params: dict[str, str] | None = None,
        *,
        check_code: bool = True,
    ) -> dict[str, Any]:
        if self.session is None or self.session.closed:
            raise RuntimeError("插件 HTTP 会话尚未初始化")
        async with self.session.get(url, params=params) as response:
            response.raise_for_status()
            payload = await response.json(content_type=None)
        if not isinstance(payload, dict):
            raise ValueError("B 站 API 返回格式异常")
        code = payload.get("code", 0)
        if check_code and code != 0:
            raise RuntimeError(f"B 站 API 返回错误 code={code}: {payload.get('message', '')}")
        return payload

    async def _get_mixin_key(self) -> str:
        async with self.wbi_lock:
            if self.mixin_key and time.time() - self.mixin_key_time < 6 * 60 * 60:
                return self.mixin_key
            # /nav 有时会返回登录相关的非零状态码，但 data 中仍包含 WBI 密钥。
            payload = await self._api_json(NAV_API, check_code=False)
            wbi_img = payload.get("data", {}).get("wbi_img", {})
            img_url = wbi_img.get("img_url", "")
            sub_url = wbi_img.get("sub_url", "")
            if not img_url or not sub_url:
                raise RuntimeError("无法从 B 站获取 WBI 签名密钥")
            img_key = img_url.rsplit("/", 1)[-1].split(".", 1)[0]
            sub_key = sub_url.rsplit("/", 1)[-1].split(".", 1)[0]
            source = img_key + sub_key
            self.mixin_key = "".join(source[index] for index in MIXIN_TABLE)[:32]
            self.mixin_key_time = time.time()
            return self.mixin_key

    async def _signed_params(self, params: dict[str, str]) -> dict[str, str]:
        signed = dict(params)
        signed["wts"] = str(int(time.time()))
        ordered = dict(sorted(signed.items()))
        query = urllib.parse.urlencode(ordered)
        for char in "!'()*":
            query = query.replace(urllib.parse.quote(char), "")
        signed["w_rid"] = hashlib.md5(
            (query + await self._get_mixin_key()).encode("utf-8")
        ).hexdigest()
        return signed

    async def _latest_videos(
        self, mid: str, *, count: int
    ) -> list[dict[str, str]]:
        params = await self._signed_params(
            {"mid": mid, "ps": str(count), "pn": "1", "order": "pubdate"}
        )
        payload = await self._api_json(VIDEO_API, params)
        vlist = payload.get("data", {}).get("list", {}).get("vlist", [])
        videos: list[dict[str, str]] = []
        for item in vlist:
            bvid = str(item.get("bvid", ""))
            if not bvid:
                continue
            videos.append(
                {
                    "bvid": bvid,
                    "title": self._clean_text(str(item.get("title", "无标题"))),
                    "description": self._clean_text(str(item.get("description", ""))),
                    "pubdate": str(item.get("created", "0")),
                }
            )
        return videos

    async def _send_notification_messages(
        self, origin: str, name: str, video: dict[str, str]
    ) -> tuple[bool, bool]:
        """先单发视频信息；成功后直接单发视频文件，失败时单发错误原因。"""
        message = self._format_video_message(name, video)
        sent = await self.context.send_message(origin, MessageChain([Plain(message)]))
        if sent is False:
            return False, False
        if not self.send_video_file:
            return True, False

        try:
            video_path = await self._get_video_path_from_parser(video["bvid"])
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                f"[BiliUPPush] 获取视频文件失败 BV={video.get('bvid', '')}: {exc}"
            )
            await self._send_notification_status(
                origin,
                self._safe_error_detail(exc),
            )
            return True, False

        try:
            sent = await self.context.send_message(
                origin, MessageChain([Video.fromFileSystem(str(video_path))])
            )
            if sent is False:
                raise RuntimeError("发送接口返回失败")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                f"[BiliUPPush] 发送视频文件失败 BV={video.get('bvid', '')}: {exc}"
            )
            await self._send_notification_status(
                origin,
                self._safe_error_detail(exc),
            )
            return True, False
        return True, True

    async def _send_notification_status(self, origin: str, message: str) -> bool:
        """单独发送附件处理状态；状态发送失败不阻断视频处理。"""
        try:
            sent = await self.context.send_message(
                origin, MessageChain([Plain(message)])
            )
            if sent is False:
                logger.warning("[BiliUPPush] 视频文件状态消息发送失败")
                return False
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(f"[BiliUPPush] 视频文件状态消息发送异常: {exc}")
            return False

    @staticmethod
    def _safe_error_detail(exc: Exception) -> str:
        """给群消息展示错误原因，同时隐藏签名地址、凭据和本机路径。"""
        if isinstance(exc, OSError):
            code = getattr(exc, "winerror", None) or exc.errno
            detail = exc.strerror or "本地文件操作失败"
            code_text = f"WinError {code}" if getattr(exc, "winerror", None) else f"错误码 {code}"
            return f"{type(exc).__name__}（{code_text}）：{detail}"

        if isinstance(exc, aiohttp.ClientResponseError):
            host = ""
            if exc.request_info is not None:
                host = exc.request_info.real_url.host or ""
            detail = f"HTTP {exc.status} {exc.message}".strip()
            return f"{detail}（主机：{host}）" if host else detail

        detail = str(exc)
        detail = re.sub(r"https?://[^\s'\"<>]+", "[远程地址已隐藏]", detail)
        detail = re.sub(r"(?i)\b[A-Z]:\\[^\s'\"<>]+", "[本机路径已隐藏]", detail)
        detail = re.sub(
            r"(?i)\b(?:SESSDATA|bili_jct|access_token|refresh_token|token|w_rid|sign)\s*[=:]\s*[^&\s,;]+",
            "[凭据已隐藏]",
            detail,
        )
        detail = detail[:500]
        return f"{type(exc).__name__}: {detail}" if detail else type(exc).__name__

    def _get_parser_plugin(self) -> Any:
        """获取万能解析器实例，并检查 PR #223 提供的公开解析接口。"""
        get_all_stars = getattr(self.context, "get_all_stars", None)
        if not callable(get_all_stars):
            raise RuntimeError("当前 AstrBot 版本无法查询已加载插件")

        parser_plugin = next(
            (
                getattr(metadata, "star_cls", None)
                for metadata in get_all_stars()
                if getattr(metadata, "name", None) == PARSER_PLUGIN_ID
                and getattr(metadata, "activated", False)
            ),
            None,
        )
        if parser_plugin is None:
            raise RuntimeError("万能解析器未安装或未启用")
        if not callable(getattr(parser_plugin, "parse_url", None)):
            raise RuntimeError(
                "万能解析器版本尚未提供 parse_url 公开接口，请更新到包含 PR #223 的版本"
            )
        return parser_plugin

    async def _get_video_path_from_parser(self, bvid: str) -> Path:
        """调用万能解析器公开接口解析并获取视频文件，不自行请求视频流。"""
        if not re.fullmatch(r"BV[0-9A-Za-z]{10}", bvid):
            raise ValueError("B 站视频编号格式无效")

        parser_plugin = self._get_parser_plugin()
        result = await parser_plugin.parse_url(f"https://www.bilibili.com/video/{bvid}")
        video_contents = getattr(result, "video_contents", None)
        if video_contents is None:
            video_contents = [
                content
                for content in getattr(result, "contents", [])
                if callable(getattr(content, "get_path", None))
            ]
        if not video_contents:
            raise RuntimeError("万能解析器没有返回可发送的视频内容")

        path = Path(await video_contents[0].get_path())
        if not path.is_file() or path.stat().st_size == 0:
            raise RuntimeError("万能解析器返回的视频文件不存在或为空")
        return path

    async def _uploader_name(self, mid: str) -> str:
        params = await self._signed_params({"mid": mid})
        payload = await self._api_json(USER_API, params)
        return self._clean_text(str(payload.get("data", {}).get("name", "")))

    async def _resolve_uploader_name(self, query: str) -> tuple[str, str]:
        """搜索 UP 主并仅接受昵称完全匹配的结果。"""
        params = {
            "search_type": "bili_user",
            "keyword": query,
            "page": "1",
            "order": "fans",
            "order_sort": "0",
        }
        try:
            signed_params = await self._signed_params(params)
            candidates = [(SEARCH_API, signed_params), (SEARCH_API_LEGACY, params)]
        except Exception as exc:
            logger.warning(
                f"[BiliUPPush] 用户搜索无法生成 WBI 签名，尝试旧接口，异常类型={type(exc).__name__}"
            )
            candidates = [(SEARCH_API_LEGACY, params)]
        exact_matches: dict[str, str] = {}
        successful_search = False
        last_error: Exception | None = None

        for endpoint, request_params in candidates:
            try:
                payload = await self._api_json(endpoint, request_params)
                successful_search = True
            except Exception as exc:
                last_error = exc
                continue
            response_data = payload.get("data") or {}
            results = response_data.get("result") or []
            for result in results if isinstance(results, list) else []:
                if not isinstance(result, dict):
                    continue
                uname = self._strip_search_highlight(str(result.get("uname", "")))
                mid = str(result.get("mid", ""))
                if uname == query and mid.isdigit():
                    exact_matches[mid] = uname

        if len(exact_matches) == 1:
            mid, name = next(iter(exact_matches.items()))
            return mid, name
        if len(exact_matches) > 1:
            uids = ", ".join(exact_matches)
            raise LookupError(f"找到多个昵称完全匹配【{query}】的 UP 主，请改用 UID：{uids}")
        if not successful_search and last_error:
            raise RuntimeError("B 站搜索接口请求失败") from last_error
        raise LookupError(f"没有找到昵称完全匹配【{query}】的 UP 主，请检查用户名或改用 UID。")

    @classmethod
    def _strip_search_highlight(cls, value: str) -> str:
        without_tags = re.sub(r"</?em\b[^>]*>", "", value, flags=re.IGNORECASE)
        return cls._clean_text(html.unescape(without_tags))

    @staticmethod
    def _clean_text(value: str) -> str:
        return re.sub(r"\s+", " ", value).strip()

    @staticmethod
    def _format_video_message(name: str, video: dict[str, str]) -> str:
        lines = [f"🔔 UP主【{name}】发布了新视频", f"📺 {video['title']}"]
        if video.get("pubdate", "0").isdigit() and int(video["pubdate"] or 0) > 0:
            published = time.strftime("%Y-%m-%d %H:%M", time.localtime(int(video["pubdate"])))
            lines.append(f"📅 发布：{published}")
        description = video.get("description", "")
        if description:
            if len(description) > 180:
                description = description[:180].rstrip() + "…"
            lines.append(f"📝 简介：{description}")
        lines.append(f"🔗 https://www.bilibili.com/video/{video['bvid']}")
        return "\n".join(lines)
