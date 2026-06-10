# Dedicated Worker Gateway

Run your own worker gateway at `gateway.firefoxnode.com` (or any host) so workers connect only to your orchestrator pool.

## Architecture

| Component | URL |
| --------- | --- |
| Worker WebSocket | `wss://gateway.firefoxnode.com/ws/{worker_id}?api_key=...&hotkey=...&region=europe` |
| Orchestrator control | `wss://gateway.firefoxnode.com/control` + header `x-control-secret` |
| Orchestrator ↔ BeamCore | `ORCH_GATEWAY_URL` (unchanged) |
| Worker payment evidence | `CORE_SERVER_URL` HTTP (unchanged) |

## 1. Start the gateway (port 8001)

```bash
cd neurons/worker_gateway
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install fastapi uvicorn websockets pydantic-settings

cp .env.example .env
# Edit WORKER_GATEWAY_CONTROL_SECRET and WORKER_GATEWAY_PUBLIC_URL

python main.py
```

Health check: `curl https://gateway.firefoxnode.com/health`

## 2. Caddy (TLS → localhost:8001)

```caddyfile
gateway.firefoxnode.com {
    reverse_proxy 127.0.0.1:8001 {
        header_up X-Forwarded-For {remote_host}
        header_up X-Real-IP {remote_host}
    }
}
```

Reload Caddy after DNS points to this host.

## 3. Configure the orchestrator

In `neurons/orchestrator/.env`:

```bash
CORE_SERVER_URL=https://beamcore.b1m.ai
ORCH_GATEWAY_URL=https://orch-gateway.b1m.ai

WORKER_GATEWAY_PUBLIC_URL=https://gateway.firefoxnode.com
WORKER_GATEWAY_CONTROL_URL=https://gateway.firefoxnode.com
WORKER_GATEWAY_CONTROL_SECRET=<same as gateway .env>

READY=true
```

Restart the orchestrator. On startup it will:

- Register `gateway_url` with BeamCore
- Connect to `/control` on your gateway
- Use **connected workers** for `chunk_assignments` (not `list_public_workers`)
- Relay `worker_task_offer` → workers and `worker_response` / `task_result_summary` → BeamCore

## 4. Point workers at your gateway

```bash
export CORE_SERVER_URL=https://beamcore.b1m.ai
export WORKER_GATEWAY_URL=https://gateway.firefoxnode.com

# Required for dedicated gateway metadata (stored on connect):
# WORKER_REGION=europe
# WORKER_REGION=north-america
# WORKER_REGION=asia

python neurons/worker/worker.py --wallet.name my_wallet --wallet.hotkey my_hotkey
```

Workers still register via BeamCore HTTP; task offers arrive over **your** WebSocket.

### Worker metadata on connect

When a worker opens `/ws/{worker_id}`, the gateway stores:

| Field | Source |
| ----- | ------ |
| **IP** | `X-Forwarded-For` / `X-Real-IP` (from Caddy) or direct socket |
| **hotkey** | Query param from worker wallet (`hotkey=...`) |
| **region** | `WORKER_REGION` env → query param; must normalize to `europe`, `north-america`, or `asia` |

Data is persisted in `worker_metrics.json` and exposed on `GET /workers` (control secret). The orchestrator receives `worker_connected` / `worker_stats_update` rows with these fields.

Invalid or missing region is stored as `unknown` until a valid region is sent on reconnect.

## Checklist (orch-owned relay)

- [ ] Gateway `/health` shows `control_connected: true` while orchestrator runs
- [ ] Worker logs `[WS] Connected!` to your domain
- [ ] Orchestrator logs `Dedicated worker gateway enabled`
- [ ] After a task: BeamCore receives `worker_response` then `task_result_summary` (orchestrator relays)
- [ ] Worker posts payment evidence only after `task_result_summary_ack.received=true`

## systemd (optional)

**Gateway** — `/etc/systemd/system/beam-worker-gateway.service`:

```ini
[Unit]
Description=BEAM Worker Gateway
After=network.target

[Service]
Type=simple
User=beam
WorkingDirectory=/path/to/beam/neurons/worker_gateway
EnvironmentFile=/path/to/beam/neurons/worker_gateway/.env
ExecStart=/path/to/beam/neurons/worker_gateway/.venv/bin/python main.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## Troubleshooting

| Symptom | Fix |
| ------- | --- |
| **HTTP 502** on `/health` or worker WebSocket | Gateway process not running on port 8001, or Caddy `reverse_proxy` points at the wrong host/port. Run `curl http://127.0.0.1:8001/health` on the Caddy host first. |
| Worker WebSocket **4401** after connect | Missing `?api_key=` — worker must register with BeamCore first; or set `GATEWAY_REQUIRE_API_KEY=false` only for debugging. |
| `409 task_not_completed` on payment evidence | Orchestrator not relaying `worker_response` / `task_result_summary` — check control secret and orch logs |
| `persist_timeout` on `task_result_summary_ack` | BeamCore did not persist the relayed result in time. Restart orch/worker after updating (relay now includes `chunk_index`, `assignment_id`, `bytes_relayed`). If it persists, open a BeamCore ticket with task/transfer IDs — your worker already moved the bytes. |
| Transfer expires after successful chunk | Same as `persist_timeout` — transfer stays open until BeamCore accepts `task_result_summary` with `received=true` |
| Worker `Task result ack timeout` | Raise `WORKER_TASK_RESULT_ACK_TIMEOUT` (default 15s) or fix orch forwarding `task_result_summary_ack` to gateway |
| `control_connected: false` | `WORKER_GATEWAY_CONTROL_SECRET` mismatch or orchestrator cannot reach control URL |
| No tasks | `READY=true`, workers connected, orchestrator registered on subnet 105 |

**502 almost always means Caddy cannot reach uvicorn.** Start the gateway before testing workers:

```bash
cd neurons/worker_gateway && python main.py
# In another shell on the same host as Caddy:
curl -s http://127.0.0.1:8001/health
```
