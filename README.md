# xiaozhiBridge

基于 FastAPI 启动 Web 服务，并在应用启动时自动拉起桥接后台任务：把小智 WebSocket 消息转发到本地 MCP Streamable HTTP 服务，并将 HTTP/SSE 响应回推到 WebSocket。现在支持多个用户，每个 `endpoint_url` 对应一个独立的桥接用户。

## 已迁移内容

- `app.py`：FastAPI 入口与生命周期管理
- `bridge.py`：桥接主逻辑
- `.env.example`：环境变量模板
- `requirements.txt`：依赖清单

## 配置

1. 复制环境变量模板：

```powershell
Copy-Item .env.example .env
```

2. 编辑 `.env`，至少填写：

- `XIAOZHI_TOKEN`，或
- `XIAOZHI_ENDPOINT_URL`

3. 确认本地 MCP 服务已启动，并且 `MCP_HTTP_URL` 正确。

## 安装依赖

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

如果你当前没有虚拟环境，也可以使用系统 Python：

```powershell
python -m pip install -r requirements.txt
```

## 启动

推荐使用 Uvicorn：

```powershell
.\.venv\Scripts\python.exe -m uvicorn app:app --host 127.0.0.1 --port 8765
```

或：

```powershell
python -m uvicorn app:app --host 127.0.0.1 --port 8765
```

也可以直接运行：

```powershell
python .\app.py
```

## 说明

- 为了避免把敏感信息提交到仓库，真实 token 不再硬编码在代码里。
- FastAPI 启动时会自动创建桥接后台任务。
- FastAPI 关闭时会自动停止桥接任务并释放 HTTP / WebSocket 资源。
- 如果没有配置 `XIAOZHI_TOKEN` 或 `XIAOZHI_ENDPOINT_URL`，FastAPI 仍可启动，但桥接服务会显示为禁用状态。
- 可以通过接口动态添加多个用户，每个用户使用自己的 `endpoint_url`。
- 程序支持：
  - WebSocket 自动重连
  - MCP Session ID 透传
  - 普通 JSON 响应转发
  - SSE `data:` 事件转发
- 如果设置了 `XIAOZHI_ENDPOINT_URL`，程序会优先使用完整地址；否则会基于 `XIAOZHI_WS_BASE_URL + token` 自动拼接。

## 最小验证

1. 本地 MCP 服务启动在 `http://127.0.0.1:8000/mcp`
2. `.env` 中已配置小智 token
3. 启动 FastAPI：`python -m uvicorn app:app --host 127.0.0.1 --port 8765`
4. 访问 `http://127.0.0.1:8765/health`
5. 观察日志中是否出现：
   - `Application startup complete`
   - `Connected to Xiaozhi WebSocket`
   - `WS -> HTTP:`
   - `HTTP response:`

## 动态新增用户

新增一个用户桥接：

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8765/bridges -ContentType "application/json" -Body '{"user_id":"u_1001","xiaozhi_device_id":"device_a1","endpoint_url":"wss://api.xiaozhi.me/mcp/?token=your_token_here"}'
```

查看所有用户：

```powershell
Invoke-RestMethod -Method Get -Uri http://127.0.0.1:8765/bridges
```

返回中会包含：

- `user_id`：你传入的业务用户标识
- `xiaozhi_device_id`：你传入的小智设备标识
- `endpoint_url_masked`：打码后的 WebSocket 地址
- `status`：当前桥接状态
- `task_running`：后台任务是否还在运行
- `websocket_connected`：WebSocket 是否已连接
- `last_error`：最近一次连接或处理错误
- `reconnect_count`：当前重新连接的尝试次数
- `max_reconnect_count`：最大允许的重新连接次数（默认为10）
- `reconnect_failed`：是否已因达到最大重连次数而失败

## 重连机制

每个用户的桥接服务会在连接失败时自动重新连接。当重新连接尝试次数超过10次后，系统会停止自动重连，标记为失败状态。

如果连接成功，重连计数会被重置为0。

## 手动触发重新连接

如果某个用户的桥接因失败被暂停了，可以通过以下接口手动触发重新连接：

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8765/bridges/{user_id}/reconnect
```

这个接口会：
1. 停止现有的桥接服务
2. 重置重连计数
3. 创建新的桥接服务重新尝试连接

健康检查会返回类似结果：

```json
{
  "api": "ok",
  "bridge_enabled": true,
  "total_users": 1,
  "running_users": 1,
  "connected_users": 1,
  "mcp_http_url": "http://127.0.0.1:8000/mcp",
  "bridge_error": null,
  "users": [
    {
      "user_id": "user_123456789abc",
      "endpoint_url_masked": "wss://api.xiaozhi.me/mcp/?token=%2A%2A%2A",
      "status": "connected",
      "task_running": true,
      "http_session_ready": true,
      "websocket_connected": true,
      "last_error": null
    }
  ]
}
```

默认把 FastAPI 放在 `8765` 端口，是为了避免和本地 MCP 默认的 `8000` 端口冲突。

## 注意

如果你想临时使用完整 WebSocket 地址，请把它放进 `.env`：

```dotenv
XIAOZHI_ENDPOINT_URL=wss://api.xiaozhi.me/mcp/?token=your_token_here
```

不要把真实 token 提交到 Git。
