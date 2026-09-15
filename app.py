from contextlib import asynccontextmanager
import os
from pathlib import Path
from typing import Any, cast

import  requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from bridge import BridgeConfig, BridgeManager, configure_logging, logger

MISSING_CREDENTIALS_MESSAGE = (
    "No default bridge configured. Add users with POST /bridges or set XIAOZHI_TOKEN / XIAOZHI_ENDPOINT_URL."
)


class BridgeCreateRequest(BaseModel):
    user_id: str = Field(..., description="Business user ID")
    xiaozhi_device_id: str = Field(..., description="Xiaozhi device ID")
    endpoint_url: str = Field(..., description="User-specific Xiaozhi WebSocket endpoint URL")


class BridgeCreateResponse(BaseModel):
    created: bool
    user: dict


def load_environment() -> None:
    env_path = Path(__file__).with_name(".env")
    load_dotenv(env_path, override=False)


def get_app_state() -> Any:
    return cast(Any, getattr(app, "state"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_environment()
    configure_logging(os.getenv("LOG_LEVEL", "INFO").strip().upper())
    app_state = cast(Any, getattr(app, "state"))
    manager = BridgeManager()

    app_state.bridge_manager = manager
    app_state.default_bridge = None
    app_state.bridge_config = None
    app_state.bridge_enabled = False
    app_state.bridge_error = None

    if BridgeConfig.has_credentials():
        config = BridgeConfig.from_env()
        # default_bridge, _ = await manager.add_default_bridge()
        # app_state.default_bridge = default_bridge
        app_state.bridge_config = config
        app_state.bridge_enabled = True

        agents = requests.get('http://127.0.0.1:8000/api/mcp/ai-agent-list')
        if agents.status_code == 200:
            if agents.json().get('code') == 0:
                agent_list = agents.json().get('data', [])
                for agent in agent_list:
                    user_id = agent.get('user_id')
                    xiaozhi_device_id = agent.get('id')
                    endpoint_url = agent.get('endpoint_url')
                    if user_id and xiaozhi_device_id and endpoint_url:
                        try:
                            await manager.add_endpoint_url(
                                user_id=str(user_id),
                                xiaozhi_device_id=str(xiaozhi_device_id),
                                endpoint_url=endpoint_url,
                            )
                        except ValueError as exc:
                            logger.error(f"Failed to add bridge for user {user_id}: {exc}")

        #TODO 这里获取api接口的待连接列表，循环遍历
    else:
        app_state.bridge_error = MISSING_CREDENTIALS_MESSAGE

    try:
        yield
    finally:
        await manager.stop_all()


app = FastAPI(
    title="xiaozhiBridge",
    lifespan=lifespan,
)


@app.get("/")
async def root() -> dict:
    app_state = get_app_state()
    manager = cast(BridgeManager, getattr(app_state, "bridge_manager"))
    summary = manager.health_snapshot()

    return {
        "service": "xiaozhiBridge",
        "status": "running",
        "bridge_enabled": summary["total_users"] > 0,
        "total_users": summary["total_users"],
    }


@app.get("/health")
async def health() -> dict:
    app_state = get_app_state()
    manager = cast(BridgeManager, getattr(app_state, "bridge_manager"))
    summary = manager.health_snapshot()

    return {
        "api": "ok",
        "bridge_enabled": summary["total_users"] > 0,
        "total_users": summary["total_users"],
        "running_users": summary["running_users"],
        "connected_users": summary["connected_users"],
        "mcp_http_url": getattr(getattr(app_state, "bridge_config", None), "mcp_http_url", None),
        "bridge_error": getattr(app_state, "bridge_error", None),
        "users": summary["users"],
    }


@app.get("/bridges")
async def list_bridges() -> dict:
    app_state = get_app_state()
    manager = cast(BridgeManager, getattr(app_state, "bridge_manager"))
    return {"users": manager.list_users()}


@app.post("/bridges", response_model=BridgeCreateResponse)
async def create_bridge(payload: BridgeCreateRequest) -> BridgeCreateResponse:
    app_state = get_app_state()
    manager = cast(BridgeManager, getattr(app_state, "bridge_manager"))

    try:
        user_snapshot, created = await manager.add_endpoint_url(
            user_id=payload.user_id,
            xiaozhi_device_id=payload.xiaozhi_device_id,
            endpoint_url=payload.endpoint_url,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    app_state.bridge_enabled = True
    app_state.bridge_error = None
    return BridgeCreateResponse(created=created, user=user_snapshot)


@app.post("/bridges/{user_id}/reconnect")
async def reconnect_bridge(user_id: str) -> dict:
    app_state = get_app_state()
    manager = cast(BridgeManager, getattr(app_state, "bridge_manager"))

    user_snapshot, success = await manager.reconnect_user(user_id)

    if not success:
        raise HTTPException(status_code=404, detail=user_snapshot.get("error", "User not found"))

    return {"success": True, "user": user_snapshot}


if __name__ == '__main__':
    import uvicorn

    load_environment()
    uvicorn.run(
        "app:app",
        host=os.getenv("APP_HOST", "127.0.0.1"),
        port=int(os.getenv("APP_PORT", "8765")),
        reload=False,
    )
