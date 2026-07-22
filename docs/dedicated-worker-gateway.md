# Dedicated Worker Gateway

Run your own worker gateway at `gateway.firefoxnode.com` (or any host) so workers connect only to your orchestrator pool.

## Architecture

| Component | URL |
| --------- | --- |
| Worker WebSocket | `wss://gateway.firefoxnode.com/ws/{worker_id}?api_key=...&hotkey=...&region=europe` |
| Orchestrator control | `wss://gateway.firefoxnode.com/control` + header `x-control-secret` |
| Worker list (private) | `GET /get-firefox-workers` + header `x-control-secret` |
| Orchestrator ↔ BeamCore | `ORCH_GATEWAY_URL` — NATS control session, `nats://` or `tls://` (e.g. `tls://orch-gateway.b1m.ai:4222`) |

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

List workers (requires secret):

```bash
curl -s -H "x-control-secret: YOUR_SECRET" https://gateway.firefoxnode.com/get-firefox-workers
```

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
ORCH_GATEWAY_URL=tls://orch-gateway.b1m.ai:4222

WORKER_GATEWAY_PUBLIC_URL=https://gateway.firefoxnode.com
WORKER_GATEWAY_CONTROL_URL=https://gateway.firefoxnode.com
WORKER_GATEWAY_CONTROL_SECRET=<same as gateway .env>

READY=true
```

Restart the orchestrator. On startup it will:

- Register `gateway_url` with BeamCore
- Connect to `/control` on your gateway
- Receive task offer batches from BeamCore over NATS and dispatch `task_offer` to connected workers
- Relay `task_result` from workers to BeamCore (retrying until a terminal status), then relay the `task_result_ack` back to the worker

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

Data is persisted in `worker_metrics.json` and exposed on `GET /get-firefox-workers` (requires `x-control-secret`). The orchestrator receives `worker_connected` / `worker_stats_update` rows with these fields.

Invalid or missing region is stored as `unknown` until a valid region is sent on reconnect.

## Checklist (orch-owned relay)

- [ ] `GET /get-firefox-workers` shows `control_connected: true` while orchestrator runs
- [ ] Worker logs `[WS] Connected!` to your domain
- [ ] Orchestrator logs `Dedicated worker gateway enabled`
- [ ] After a task: worker sends `task_result`, orchestrator relays to BeamCore, worker gets a terminal `task_result_ack`

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
| **HTTP 502** on worker WebSocket | Gateway process not running on port 8001, or Caddy `reverse_proxy` points at the wrong host/port. Confirm `python main.py` is running on the Caddy host. |
| Worker WebSocket **4401** after connect | Missing `?api_key=` — worker must register with BeamCore first; or set `GATEWAY_REQUIRE_API_KEY=false` only for debugging. |
| `task_result` never reaches a terminal ack | Orchestrator not relaying `task_result` to BeamCore, or BeamCore hasn't reached a terminal status yet — check control secret and orch logs; the relay retries automatically until terminal |
| Worker `Task result ack timeout` | Raise `WORKER_TASK_RESULT_ACK_TIMEOUT` or fix orch forwarding `task_result_ack` to the gateway |
| `control_connected: false` | `WORKER_GATEWAY_CONTROL_SECRET` mismatch or orchestrator cannot reach control URL |
| No tasks | `READY=true`, workers connected, orchestrator registered on subnet 105 |

**502 almost always means Caddy cannot reach uvicorn.** Start the gateway before testing workers:

```bash
cd neurons/worker_gateway && python main.py
# In another shell on the same host as Caddy:
curl -s -H "x-control-secret: YOUR_SECRET" http://127.0.0.1:8001/get-firefox-workers
```
