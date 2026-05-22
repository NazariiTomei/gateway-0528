# Beam Validator

Validators read scoring inputs from the public BEAM control plane and set weights on Bittensor subnet 105.

## Install

From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[validator]"
```

## Configure

```bash
BEAM_VALIDATOR_CORE_SERVER_URL=https://beamcore.b1m.ai
SUBTENSOR_NETWORK=finney
NETUID=105
BEAM_VALIDATOR_WALLET_NAME=your_coldkey
BEAM_VALIDATOR_WALLET_HOTKEY=your_hotkey
```

## Run

```bash
cd neurons/validator
python main.py
```

## Environment

| Variable | Description | Default |
| -------- | ----------- | ------- |
| `BEAM_VALIDATOR_CORE_SERVER_URL` | Beam control-plane HTTP base | `https://beamcore.b1m.ai` |
| `SUBTENSOR_NETWORK` | Bittensor network | `finney` |
| `NETUID` | BEAM subnet UID | `105` |
| `BEAM_VALIDATOR_WALLET_NAME` | Bittensor coldkey name | `default` |
| `BEAM_VALIDATOR_WALLET_HOTKEY` | Bittensor hotkey name | `default` |

Validator routes are rooted at `BEAM_VALIDATOR_CORE_SERVER_URL` with no `/api` prefix.

See [../../docs/validator.md](../../docs/validator.md) for the full operator guide.
