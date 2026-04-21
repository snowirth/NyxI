# Nyx

Nyx is an autonomous, persistent personal operator that remembers your world, follows through on goals, and can safely grow how it works over time.

The whole thing runs locally. It only knows what you teach it.

## What Nyx Does

- Talks through web, Telegram, Discord, voice, and MCP.
- Uses fast deterministic paths for simple requests and tool-calling chat for open-ended work.
- Remembers what matters — conversations, facts, corrections, preferences — and surfaces it when relevant.
- Tracks goals, blockers, and ongoing work through a DB-backed autonomy loop.
- Builds its own Python tools when it hits a gap and can call them immediately.
- Edits its own protected core files with backup, verification, and rollback.
- Runs background work for awareness, overnight reflection, file watching, and autonomy.

## Loop

The core Nyx loop is:

`remember -> decide -> act -> verify -> learn`


## Setup

The fastest successful local Nyx run is the operator path, not just a compile:

```bash
./scripts/first_run_local.sh
./scripts/first_run_local.sh --smoke
./scripts/first_run_local.sh --run
```

That gives you the first real "Nyx is here" moment:
- `.env` exists with practical local defaults
- Nyx builds
- the web runtime answers on `http://127.0.0.1:8099/health`
- the operator brief endpoint answers with the generated bearer token

What you need installed:
- Rust + Cargo
- `curl` for `--smoke`
- Ollama only if you want the easiest fully local model path

That script:
- creates `.env` from `.env.example` if needed
- fills in safer first-run defaults like `NYX_WEB_PORT`, `NYX_USER_NAME`, and `NYX_API_TOKEN`
- detects a local Ollama model from `ollama list` when possible
- generates `NYX_API_TOKEN` if it is missing
- explains the next provider step if Nyx still needs one LLM decision
- builds Nyx and can optionally smoke-check the web runtime

If the script says no provider is ready yet, do one of these and rerun it:
- fastest fully local path: install or pull one Ollama model
- hosted path: set `NYX_ANTHROPIC_API_KEY`
- hosted path: set `NYX_NIM_API_KEY`

Manual path:

```bash
git clone git@github.com:pvlata75/Nyx.git
cd Nyx
cp .env.example .env
# then set one provider before your first real run:
# - NYX_ANTHROPIC_API_KEY=...
# - or NYX_NIM_API_KEY=...
# - or NYX_OLLAMA_MODEL=<exact name from `ollama list`>
cargo build --release
./target/release/nyx
```

Web UI defaults to `http://127.0.0.1:8099`. Telegram and Discord come online only if their tokens are set.
The smoke step also checks `GET /api/operator/brief`, which is the quickest trust check that Nyx is up as an operator and not only as a background process.

## What To Write In `.env`

Pick one provider. For the fastest fully local path, leave `NYX_OLLAMA_MODEL` empty in `.env.example` and let `./scripts/first_run_local.sh` detect a model already present in `ollama list` and write it into `.env`. If you skip the script, set `NYX_OLLAMA_MODEL` yourself to an installed model name.

```dotenv
NYX_CHAT_PRIMARY=                  # leave empty for auto; script fills this when it can
NYX_ANTHROPIC_API_KEY=
NYX_ANTHROPIC_MODEL=
NYX_NIM_API_KEY=
NYX_NIM_MODEL=
NYX_NIM_BASE_URL=                  # default: https://integrate.api.nvidia.com
NYX_OLLAMA_HOST=http://127.0.0.1:11434
NYX_OLLAMA_MODEL=                  # set manually only if you are not using the bootstrap script
```

Useful first-run knobs:

```dotenv
NYX_WEB_PORT=8099
NYX_API_TOKEN=                     # leave empty and the bootstrap script will generate one
NYX_USER_NAME=
NYX_USER_LOCATION=
NYX_DEFAULT_CITY=
NYX_GITHUB_REPO=
NYX_TELEGRAM_TOKEN=
NYX_TELEGRAM_OWNER_IDS=
NYX_DISCORD_TOKEN=
NYX_OS_SANDBOX_MODE=auto           # auto | off | force
NYX_SELF_EDIT_VERIFY_MODE=         # syntax (faster) or empty (full cargo test)
NYX_OVERNIGHT_SPEED=
```

## Running

- `./target/release/nyx` — full runtime (web + enabled bots).
- `./target/release/nyx --mcp` — MCP server mode.

## License

MIT
