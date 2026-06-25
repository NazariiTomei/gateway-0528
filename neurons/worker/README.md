# Beam Network Worker

A worker node for the Beam Network — an open coordination layer for distributed data transfer built on Bittensor.

Workers receive data transfer tasks, fetch chunks from a source, deliver them to a destination, and report completion with bandwidth metrics.

## Requirements

- Python 3.10+
- CPU: 2+ cores
- RAM: 4 GB+
- Storage: 20 GB SSD
- Network: 100 Mbps symmetric (upload/download)
- OS: Ubuntu 22.04+ / Debian 12+ / macOS 13+

## Installation

From `beam/` (recommended; matches subnet dependencies):

```bash
pip install -e "."
```

The worker runtime also relies on packages declared in [`pyproject.toml`](../../pyproject.toml); for a minimal manual install:

```bash
pip install bittensor httpx websockets
```

## Usage

Run from `beam/neurons/worker`:

```bash
# Default wallet
python worker.py

# Custom wallet
python worker.py --wallet.name my_wallet --wallet.hotkey my_hotkey

# Mainnet
python worker.py --subtensor.network finney
```

## Transport

The worker uses BeamCore HTTP only for registration and signed bootstrap calls. Transfer runtime uses **worker-gateway** WebSockets (`WORKER_GATEWAY_URL` must be the gateway HTTP/WebSocket origin, not BeamCore Core).

Typical environment:

```bash
export CORE_SERVER_URL=https://beamcore.b1m.ai
export WORKER_GATEWAY_URL=https://public-worker-gateway.b1m.ai
export CONNECTION_MODE=auto               # or websocket (see worker.py)
python worker.py --subtensor.network finney
```

## Task Offer Protocol

Workers read `WORKER_VERSION` from package metadata. BeamCore includes `minimum_worker_version` in every `task_offer`; workers reject offers below that SemVer with `unsupported_worker_version` before fetching source bytes or uploading destination bytes.

BeamCore also includes `signed_url_flow`. `signed_url_v1` is the default object-storage flow and requires `dest_headers.Content-MD5` on UploadPart offers. Workers reject checksum-less v1 object-storage offers before upload. `signed_url_v2` remains selectable by the transfer creator.

## How It Works

1. Registers with the network using your Bittensor wallet (signed authentication)
2. Connects to `worker-gateway` via WebSocket to receive tasks instantly as they are assigned
3. For each task: fetches data chunks from the source and delivers them to the destination
4. Reports completion with proof-of-bandwidth metrics (bytes transferred, speed, duration)
5. Sends periodic heartbeats to stay registered

## Environment Variables

| Variable            | Required | Description |
| ------------------- | -------- | ----------- |
| `CORE_SERVER_URL`   | no       | BeamCore HTTP base. |
| `WORKER_GATEWAY_URL`        | **yes**  | Worker-gateway base URL (`http(s)://host:port` — used to derive WebSocket URLs). |
| `CONNECTION_MODE`   | no       | `websocket` / `polling` / `auto` (default `websocket` in env). Transfer path expects gateway WebSockets. |
