import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed


logger = logging.getLogger("MCP_PROXY")


@dataclass(frozen=True)
class BridgeConfig:
    mcp_http_url: str
    xiaozhi_ws_base_url: str
    xiaozhi_token: str
    user_id: str
    xiaozhi_endpoint_url: Optional[str]
    log_level: str
    ws_ping_interval: int
    ws_ping_timeout: int
    reconnect_max_backoff: int
    connect_timeout: int
    max_log_chars: int

    @classmethod
    def has_credentials(cls) -> bool:
        endpoint_url = os.getenv("XIAOZHI_ENDPOINT_URL", "").strip()
        token = os.getenv("XIAOZHI_TOKEN", "").strip()
        return bool(endpoint_url or token)

    @classmethod
    def from_env(cls) -> "BridgeConfig":
        endpoint_url = os.getenv("XIAOZHI_ENDPOINT_URL", "").strip() or None
        token = os.getenv("XIAOZHI_TOKEN", "").strip()

        if not endpoint_url and not token:
            raise RuntimeError(
                "Missing Xiaozhi credentials. Set XIAOZHI_TOKEN or XIAOZHI_ENDPOINT_URL in your environment."
            )

        return cls(
            mcp_http_url=os.getenv("MCP_HTTP_URL", "http://127.0.0.1:8000/mcp").strip(),
            xiaozhi_ws_base_url=os.getenv("XIAOZHI_WS_BASE_URL", "wss://api.xiaozhi.me/mcp/").strip(),
            xiaozhi_token=token,
            user_id=os.getenv("DEFAULT_USER_ID", None),
            xiaozhi_endpoint_url=endpoint_url,
            log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
            ws_ping_interval=int(os.getenv("WS_PING_INTERVAL", "20")),
            ws_ping_timeout=int(os.getenv("WS_PING_TIMEOUT", "20")),
            reconnect_max_backoff=int(os.getenv("RECONNECT_MAX_BACKOFF", "60")),
            connect_timeout=int(os.getenv("CONNECT_TIMEOUT", "30")),
            max_log_chars=int(os.getenv("MAX_LOG_CHARS", "500")),
        )

    @classmethod
    def from_endpoint_url(cls, endpoint_url: str, user_id: str) -> "BridgeConfig":
        normalized_endpoint_url = endpoint_url.strip()
        if not normalized_endpoint_url:
            raise ValueError("endpoint_url is required.")

        return cls(
            mcp_http_url=os.getenv("MCP_HTTP_URL", "http://127.0.0.1:8001/mcp").strip(),
            xiaozhi_ws_base_url=os.getenv("XIAOZHI_WS_BASE_URL", "wss://api.xiaozhi.me/mcp/").strip(),
            xiaozhi_token="",
            user_id=user_id,
            xiaozhi_endpoint_url=normalized_endpoint_url,
            log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
            ws_ping_interval=int(os.getenv("WS_PING_INTERVAL", "20")),
            ws_ping_timeout=int(os.getenv("WS_PING_TIMEOUT", "20")),
            reconnect_max_backoff=int(os.getenv("RECONNECT_MAX_BACKOFF", "60")),
            connect_timeout=int(os.getenv("CONNECT_TIMEOUT", "30")),
            max_log_chars=int(os.getenv("MAX_LOG_CHARS", "500")),
        )

    def websocket_url(self) -> str:
        if self.xiaozhi_endpoint_url:
            return self.xiaozhi_endpoint_url

        url_parts = urlsplit(self.xiaozhi_ws_base_url)
        query = dict(parse_qsl(url_parts.query, keep_blank_values=True))
        query["token"] = self.xiaozhi_token
        return urlunsplit(
            (
                url_parts.scheme,
                url_parts.netloc,
                url_parts.path,
                urlencode(query),
                url_parts.fragment,
            )
        )


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def mask_endpoint_url(endpoint_url: str) -> str:
    url_parts = urlsplit(endpoint_url)
    query = []

    for key, value in parse_qsl(url_parts.query, keep_blank_values=True):
        if key.lower() == "token" and value:
            query.append((key, "***"))
        else:
            query.append((key, value))

    return urlunsplit(
        (
            url_parts.scheme,
            url_parts.netloc,
            url_parts.path,
            urlencode(query),
            url_parts.fragment,
        )
    )


def validate_endpoint_url(endpoint_url: str) -> str:
    normalized_endpoint_url = endpoint_url.strip()
    if not normalized_endpoint_url:
        raise ValueError("endpoint_url is required.")

    url_parts = urlsplit(normalized_endpoint_url)
    if url_parts.scheme not in {"ws", "wss"}:
        raise ValueError("endpoint_url must start with ws:// or wss://.")

    if not url_parts.netloc:
        raise ValueError("endpoint_url must include a hostname.")

    return normalized_endpoint_url


@dataclass
class ManagedBridge:
    user_id: str
    xiaozhi_device_id: str
    endpoint_url: str
    service: "BridgeService"
    task: asyncio.Task


class BridgeService:
    def __init__(self, config: BridgeConfig) -> None:
        self.config = config
        self.http_session: Optional[aiohttp.ClientSession] = None
        self._stop_event = asyncio.Event()
        self._websocket = None
        self.status = "idle"
        self.last_error: Optional[str] = None
        self.reconnect_count = 0
        self.max_reconnect_count = 10
        self.reconnect_failed = False

    async def start(self) -> None:
        if self.http_session is not None:
            return

        self._stop_event.clear()
        self.status = "starting"
        self.last_error = None

        self.http_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(
                total=None,
                connect=self.config.connect_timeout,
                sock_read=None,
            )
        )

    async def close(self) -> None:
        if self.http_session is not None:
            await self.http_session.close()
            self.http_session = None

    async def stop(self) -> None:
        self._stop_event.set()
        self.status = "stopping"
        self.reconnect_failed = False
        self.reconnect_count = 0

        if self._websocket is not None:
            await self._websocket.close()

    async def websocket_to_http(
        self,
        websocket,
        message,
        session_id: Optional[str] = None
    ) -> Optional[str]:
        if self.http_session is None:
            raise RuntimeError("HTTP session is not initialized.")

        body = message.encode("utf-8") if isinstance(message, str) else message
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }

        if session_id:
            headers["Mcp-Session-Id"] = session_id
        if self.config.user_id:
            headers["User-Id"] = self.config.user_id

        logger.info("WS -> HTTP: %s", self._preview_bytes(body))

        async with self.http_session.post(
            self.config.mcp_http_url,
            data=body,
            headers=headers,
        ) as response:
            logger.info(
                "HTTP response: %s %s",
                response.status,
                response.headers.get("Content-Type"),
            )

            new_session_id = response.headers.get("Mcp-Session-Id")
            content_type = response.headers.get("Content-Type", "").lower()

            if "text/event-stream" in content_type:
                await self._forward_sse_response(response, websocket)
            else:
                await self._forward_standard_response(response, websocket, content_type)

            return new_session_id

    async def _forward_sse_response(self, response, websocket) -> None:
        data_lines = []

        async for raw_line in response.content:
            if self._stop_event.is_set():
                break

            if not raw_line:
                continue

            line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
            logger.info("HTTP SSE -> WS: %s", line)

            if not line:
                if data_lines:
                    payload = "\n".join(data_lines).strip()
                    if payload:
                        await websocket.send(payload)
                    data_lines.clear()
                continue

            if line.startswith(":"):
                continue

            if line.startswith("data:"):
                data_lines.append(line[5:].lstrip())

        if data_lines:
            payload = "\n".join(data_lines).strip()
            if payload:
                await websocket.send(payload)

    async def _forward_standard_response(self, response, websocket, content_type: str) -> None:
        data = await response.read()
        if not data:
            return

        logger.info("HTTP -> WS: %s", self._preview_bytes(data))

        if "application/json" in content_type or content_type.startswith("text/"):
            await websocket.send(data.decode("utf-8", errors="replace"))
            return

        await websocket.send(data)

    async def connect_to_xiaozhi(self) -> None:
        session_id = None
        endpoint_url = self.config.websocket_url()

        self.reconnect_count += 1

        if self.reconnect_count > self.max_reconnect_count:
            self.status = "failed"
            self.reconnect_failed = True
            self.last_error = f"Max reconnection attempts ({self.max_reconnect_count}) exceeded"
            logger.error("Max reconnection attempts exceeded: %s", mask_endpoint_url(endpoint_url))
            raise RuntimeError(self.last_error)

        logger.info("Connecting to Xiaozhi:")
        logger.info("%s", mask_endpoint_url(endpoint_url))
        self.status = "connecting"

        async with websockets.connect(
            endpoint_url,
            max_size=None,
            ping_interval=self.config.ws_ping_interval,
            ping_timeout=self.config.ws_ping_timeout,
        ) as websocket:
            self._websocket = websocket
            self.last_error = None
            self.status = "connected"
            logger.info("Connected to Xiaozhi WebSocket")

            try:
                async for message in websocket:
                    if self._stop_event.is_set():
                        break

                    logger.info("Xiaozhi WS message: %s", self._preview_message(message))

                    try:
                        new_session_id = await self.websocket_to_http(
                            websocket,
                            message,
                            session_id,
                        )

                        if new_session_id:
                            session_id = new_session_id
                            logger.info("MCP Session ID: %s", session_id)

                    except Exception as exc:
                        logger.exception("Failed to process message: %s", exc)
            except ConnectionClosed:
                if self._stop_event.is_set():
                    self.status = "stopped"
                    logger.info("Xiaozhi WebSocket closed during shutdown")
                else:
                    raise
            finally:
                self._websocket = None
                if self._stop_event.is_set():
                    self.status = "stopped"
                elif self.status == "connected":
                    self.status = "disconnected"

    async def run_forever(self) -> None:
        await self.start()
        backoff = 1

        try:
            while not self._stop_event.is_set():
                try:
                    await self.connect_to_xiaozhi()
                    backoff = 1
                    self.reconnect_count = 0
                except asyncio.CancelledError:
                    self.status = "cancelled"
                    raise
                except Exception as exc:
                    if self.reconnect_failed or self._stop_event.is_set():
                        break

                    self.last_error = str(exc)
                    self.status = "reconnecting"
                    logger.error("WebSocket connection failed: %s", exc)
                    logger.info("Reconnect after %s seconds (attempt %d/%d)", backoff, self.reconnect_count,
                                self.max_reconnect_count)

                    try:
                        await asyncio.wait_for(self._stop_event.wait(), timeout=backoff)
                    except asyncio.TimeoutError:
                        pass

                    backoff = min(backoff * 2, self.config.reconnect_max_backoff)
        finally:
            if self._stop_event.is_set() and self.status != "cancelled":
                self.status = "stopped"
            await self.close()

    def snapshot(self) -> dict:
        endpoint_url = self.config.websocket_url()
        return {
            "endpoint_url_masked": mask_endpoint_url(endpoint_url),
            "status": self.status,
            "task_running": not self._stop_event.is_set(),
            "http_session_ready": self.http_session is not None,
            "websocket_connected": self._websocket is not None,
            "last_error": self.last_error,
            "reconnect_count": self.reconnect_count,
            "max_reconnect_count": self.max_reconnect_count,
            "reconnect_failed": self.reconnect_failed,
        }

    def _preview_message(self, message) -> str:
        if isinstance(message, str):
            return self._truncate(message)

        return self._preview_bytes(message)

    def _preview_bytes(self, data: bytes) -> str:
        return self._truncate(data.decode("utf-8", errors="replace"))

    def _truncate(self, value: str) -> str:
        if len(value) <= self.config.max_log_chars:
            return value

        return f"{value[:self.config.max_log_chars]}..."


class BridgeManager:
    def __init__(self) -> None:
        self._bridges: dict[str, ManagedBridge] = {}
        self._lock = asyncio.Lock()

    async def add_endpoint_url(
        self,
        user_id: str,
        xiaozhi_device_id: str,
        endpoint_url: str,
    ) -> tuple[dict, bool]:
        normalized_user_id = user_id.strip()
        normalized_device_id = xiaozhi_device_id.strip()
        if not normalized_user_id:
            raise ValueError("user_id is required.")
        if not normalized_device_id:
            raise ValueError("xiaozhi_device_id is required.")

        normalized_endpoint_url = validate_endpoint_url(endpoint_url)

        async with self._lock:
            existing_bridge = self._bridges.get(normalized_user_id)
            if existing_bridge is not None:
                return self._build_bridge_snapshot(existing_bridge), False

            config = BridgeConfig.from_endpoint_url(normalized_endpoint_url, user_id)
            service = BridgeService(config)
            task = asyncio.create_task(
                service.run_forever(),
                name=f"xiaozhi-bridge-{normalized_user_id}",
            )
            managed_bridge = ManagedBridge(
                user_id=normalized_user_id,
                xiaozhi_device_id=normalized_device_id,
                endpoint_url=normalized_endpoint_url,
                service=service,
                task=task,
            )
            self._bridges[normalized_user_id] = managed_bridge

            return self._build_bridge_snapshot(managed_bridge), True

    async def add_default_bridge(self) -> tuple[Optional[dict], bool]:
        if not BridgeConfig.has_credentials():
            return None, False

        config = BridgeConfig.from_env()
        endpoint_url = validate_endpoint_url(config.websocket_url())
        default_user_id = os.getenv("DEFAULT_USER_ID", "default_user")
        default_device_id = os.getenv("DEFAULT_XIAOZHI_DEVICE_ID", "default_device")
        return await self.add_endpoint_url(default_user_id, default_device_id, endpoint_url)

    async def stop_all(self) -> None:
        async with self._lock:
            bridges = list(self._bridges.values())

        for managed_bridge in bridges:
            await managed_bridge.service.stop()

        for managed_bridge in bridges:
            managed_bridge.task.cancel()

        for managed_bridge in bridges:
            try:
                await managed_bridge.task
            except asyncio.CancelledError:
                pass

    async def reconnect_user(self, user_id: str) -> tuple[dict, bool]:
        async with self._lock:
            managed_bridge = self._bridges.get(user_id)
            if managed_bridge is None:
                return {"error": f"User {user_id} not found"}, False

            await managed_bridge.service.stop()
            managed_bridge.task.cancel()

            try:
                await managed_bridge.task
            except asyncio.CancelledError:
                pass

            config = BridgeConfig.from_endpoint_url(managed_bridge.endpoint_url, user_id)
            service = BridgeService(config)
            task = asyncio.create_task(
                service.run_forever(),
                name=f"xiaozhi-bridge-{user_id}",
            )
            new_managed_bridge = ManagedBridge(
                user_id=user_id,
                xiaozhi_device_id=managed_bridge.xiaozhi_device_id,
                endpoint_url=managed_bridge.endpoint_url,
                service=service,
                task=task,
            )
            self._bridges[user_id] = new_managed_bridge

        return self._build_bridge_snapshot(new_managed_bridge), True

    def health_snapshot(self) -> dict:
        bridges = list(self._bridges.values())
        users = [self._build_bridge_snapshot(managed_bridge) for managed_bridge in bridges]
        running_users = sum(1 for user in users if user["task_running"])
        connected_users = sum(1 for user in users if user["websocket_connected"])

        return {
            "total_users": len(users),
            "running_users": running_users,
            "connected_users": connected_users,
            "users": users,
        }

    def list_users(self) -> list[dict]:
        return [self._build_bridge_snapshot(managed_bridge) for managed_bridge in self._bridges.values()]

    def _build_bridge_snapshot(self, managed_bridge: ManagedBridge) -> dict:
        service_snapshot = managed_bridge.service.snapshot()
        service_snapshot["task_running"] = not managed_bridge.task.done()

        if managed_bridge.task.done() and service_snapshot["status"] not in {"stopped", "cancelled"}:
            service_snapshot["status"] = "stopped"

        return {
            "user_id": managed_bridge.user_id,
            "xiaozhi_device_id": managed_bridge.xiaozhi_device_id,
            **service_snapshot,
        }
