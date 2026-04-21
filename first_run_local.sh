#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"
ENV_EXAMPLE="${ROOT_DIR}/.env.example"

MODE="prepare"
BUILD_MODE="release"
ENV_CREATED=0
API_TOKEN_GENERATED=0
DETECTED_OLLAMA_MODEL=""

usage() {
  cat <<'EOF'
Usage:
  ./scripts/first_run_local.sh
  ./scripts/first_run_local.sh --smoke
  ./scripts/first_run_local.sh --run
  ./scripts/first_run_local.sh --debug

What it does:
  - creates .env from .env.example if needed
  - fills in a few safer local defaults for first run
  - detects a local Ollama model when possible
  - generates NYX_API_TOKEN if missing
  - builds Nyx

Modes:
  default   prepare env + build + print next steps
  --smoke   prepare env + build + boot Nyx + wait for /health + stop
  --run     prepare env + build + run Nyx in foreground
  --debug   like --run, but uses cargo run instead of target/release/nyx
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

normalize_env_value() {
  printf '%s' "$1" | sed -E 's/[[:space:]]+#.*$//; s/^[[:space:]]+//; s/[[:space:]]+$//'
}

ensure_env_file() {
  if [[ -f "${ENV_FILE}" ]]; then
    return
  fi
  if [[ ! -f "${ENV_EXAMPLE}" ]]; then
    echo "missing ${ENV_EXAMPLE}" >&2
    exit 1
  fi
  cp "${ENV_EXAMPLE}" "${ENV_FILE}"
  ENV_CREATED=1
}

read_env_value() {
  local key="$1"
  local line
  line="$(grep -E "^${key}=" "${ENV_FILE}" | head -n 1 || true)"
  normalize_env_value "${line#*=}"
}

set_env_if_missing() {
  local key="$1"
  local value="$2"
  local tmp_file
  local found=0
  tmp_file="$(mktemp)"

  while IFS= read -r line || [[ -n "${line}" ]]; do
    if [[ "${line}" == "${key}="* ]]; then
      found=1
      local current
      current="$(normalize_env_value "${line#*=}")"
      if [[ -z "${current}" ]]; then
        printf '%s=%s\n' "${key}" "${value}" >>"${tmp_file}"
      else
        printf '%s\n' "${line}" >>"${tmp_file}"
      fi
    else
      printf '%s\n' "${line}" >>"${tmp_file}"
    fi
  done <"${ENV_FILE}"

  if [[ "${found}" == "0" ]]; then
    printf '%s=%s\n' "${key}" "${value}" >>"${tmp_file}"
  fi

  mv "${tmp_file}" "${ENV_FILE}"
}

generate_token() {
  LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c 24
}

list_ollama_models() {
  if ! command -v ollama >/dev/null 2>&1; then
    return 1
  fi
  ollama list 2>/dev/null | awk 'NR > 1 {print $1}'
}

detect_ollama_model() {
  local models
  models="$(list_ollama_models || true)"
  if [[ -z "${models}" ]]; then
    return 1
  fi

  local preferred
  preferred="$(printf '%s\n' "${models}" | grep -Ei 'gemma|qwen|deepseek|llama|mistral' | head -n 1 || true)"
  if [[ -n "${preferred}" ]]; then
    printf '%s\n' "${preferred}"
    return 0
  fi

  printf '%s\n' "${models}" | head -n 1
}

is_local_ollama_host() {
  local host="${1%/}"
  [[ -z "${host}" || "${host}" == "http://127.0.0.1:11434" || "${host}" == "http://localhost:11434" ]]
}

ollama_model_exists_locally() {
  local model="$1"
  [[ -n "${model}" ]] || return 1
  list_ollama_models | grep -Fx "${model}" >/dev/null 2>&1
}

prepare_first_run_env() {
  ensure_env_file

  set_env_if_missing "NYX_WEB_PORT" "8099"
  set_env_if_missing "NYX_USER_NAME" "$(whoami)"

  local anthropic_key nim_key chat_primary ollama_model
  anthropic_key="$(read_env_value "NYX_ANTHROPIC_API_KEY")"
  nim_key="$(read_env_value "NYX_NIM_API_KEY")"
  chat_primary="$(read_env_value "NYX_CHAT_PRIMARY")"
  ollama_model="$(read_env_value "NYX_OLLAMA_MODEL")"

  if [[ -z "$(read_env_value "NYX_API_TOKEN")" ]]; then
    set_env_if_missing "NYX_API_TOKEN" "$(generate_token)"
    API_TOKEN_GENERATED=1
  fi

  if [[ -z "${anthropic_key}" && -z "${nim_key}" ]]; then
    set_env_if_missing "NYX_OLLAMA_HOST" "http://127.0.0.1:11434"
    local detected_model=""
    if [[ -z "${ollama_model}" ]]; then
      detected_model="$(detect_ollama_model || true)"
      if [[ -n "${detected_model}" ]]; then
        set_env_if_missing "NYX_OLLAMA_MODEL" "${detected_model}"
        DETECTED_OLLAMA_MODEL="${detected_model}"
      fi
    fi
    if [[ -n "${ollama_model}" || -n "${detected_model}" ]]; then
      set_env_if_missing "NYX_CHAT_PRIMARY" "ollama"
    fi
  elif [[ -z "${chat_primary}" ]]; then
    if [[ -n "${anthropic_key}" ]]; then
      set_env_if_missing "NYX_CHAT_PRIMARY" "anthropic"
    elif [[ -n "${nim_key}" ]]; then
      set_env_if_missing "NYX_CHAT_PRIMARY" "nim"
    fi
  fi
}

print_provider_help() {
  local provider anthropic_key nim_key ollama_model ollama_host
  provider="$(read_env_value "NYX_CHAT_PRIMARY")"
  anthropic_key="$(read_env_value "NYX_ANTHROPIC_API_KEY")"
  nim_key="$(read_env_value "NYX_NIM_API_KEY")"
  ollama_model="$(read_env_value "NYX_OLLAMA_MODEL")"
  ollama_host="$(read_env_value "NYX_OLLAMA_HOST")"

  echo "Provider follow-through:"
  if [[ -n "${anthropic_key}" || -n "${nim_key}" ]]; then
    echo "  - Cloud provider credentials are present. Smoke/run should work with the resolved provider."
    return
  fi

  if [[ "${provider}" == "ollama" && -n "${ollama_model}" ]]; then
    if is_local_ollama_host "${ollama_host}"; then
      if ! command -v ollama >/dev/null 2>&1; then
        echo "  - NYX is pointed at local Ollama, but the ollama CLI is not installed."
        echo "  - Install Ollama or change NYX_OLLAMA_HOST to a running Ollama instance."
      elif ! ollama_model_exists_locally "${ollama_model}"; then
        echo "  - NYX is pointed at Ollama model '${ollama_model}', but it is not installed locally."
        echo "  - Run 'ollama list' and set NYX_OLLAMA_MODEL to one of those names."
      else
        echo "  - Local Ollama looks ready."
      fi
    else
      echo "  - Ollama is configured through NYX_OLLAMA_HOST=${ollama_host}."
      echo "  - Make sure that host is reachable before smoke/running Nyx."
    fi
    return
  fi

  if command -v ollama >/dev/null 2>&1; then
    echo "  - Fastest fully local path: install or pull one Ollama model, then rerun this script."
    echo "  - Use 'ollama list' to confirm the exact model name, or set NYX_OLLAMA_MODEL manually."
  else
    echo "  - Fastest fully local path: install Ollama, pull one model, then rerun this script."
  fi
  echo "  - Hosted path: set NYX_ANTHROPIC_API_KEY or NYX_NIM_API_KEY in .env, then rerun this script."
}

print_summary() {
  local port token provider model ollama_host
  port="$(read_env_value "NYX_WEB_PORT")"
  token="$(read_env_value "NYX_API_TOKEN")"
  provider="$(read_env_value "NYX_CHAT_PRIMARY")"
  model="$(read_env_value "NYX_OLLAMA_MODEL")"
  ollama_host="$(read_env_value "NYX_OLLAMA_HOST")"

  echo
  echo "Nyx first-run setup is ready."
  echo
  echo "Resolved first-run state:"
  if [[ "${ENV_CREATED}" == "1" ]]; then
    echo "  env file: created .env from .env.example"
  else
    echo "  env file: using existing .env"
  fi
  echo "  provider: ${provider:-not yet selected}"
  if [[ -n "${model}" ]]; then
    echo "  ollama model: ${model}"
  elif [[ -n "${DETECTED_OLLAMA_MODEL}" ]]; then
    echo "  ollama model: ${DETECTED_OLLAMA_MODEL}"
  fi
  echo "  web: http://127.0.0.1:${port}"
  echo "  api token: ${token}"
  if [[ "${API_TOKEN_GENERATED}" == "1" ]]; then
    echo "  auth: generated a fresh local bearer token"
  fi
  echo

  if [[ "${provider}" == "ollama" && -z "${model}" ]]; then
    echo "Warning:"
    echo "  NYX_CHAT_PRIMARY is set to ollama but no local model was detected."
    echo "  Set NYX_OLLAMA_MODEL in .env to a model you already have installed."
    echo
  elif [[ "${provider}" == "ollama" && -n "${model}" ]] \
    && is_local_ollama_host "${ollama_host}" \
    && ! ollama_model_exists_locally "${model}"; then
    echo "Warning:"
    echo "  NYX is configured for local Ollama model '${model}', but that model was not found in 'ollama list'."
    echo "  Update NYX_OLLAMA_MODEL in .env or pull the model locally before smoke/running Nyx."
    echo
  elif [[ -z "${provider}" ]]; then
    echo "Warning:"
    echo "  No provider was auto-selected."
    echo "  Set one of NYX_ANTHROPIC_API_KEY, NYX_NIM_API_KEY, or NYX_OLLAMA_MODEL in .env."
    echo
  fi

  print_provider_help
  echo

  echo "Fastest first success:"
  echo "  1. ./scripts/first_run_local.sh --smoke"
  echo "  2. ./scripts/first_run_local.sh --run"
  echo "  3. Open http://127.0.0.1:${port}"
}

build_nyx() {
  require_cmd cargo
  (
    cd "${ROOT_DIR}"
    if [[ "${BUILD_MODE}" == "debug" ]]; then
      cargo build
    else
      cargo build --release
    fi
  )
}

wait_for_health() {
  local base_url="$1"
  local token="$2"
  local attempts=40

  for _ in $(seq 1 "${attempts}"); do
    if curl --max-time 5 -sf "${base_url}/health" >/dev/null 2>&1; then
      if curl --max-time 5 -sf "${base_url}/api/operator/brief" \
        -H "Authorization: Bearer ${token}" >/dev/null 2>&1; then
        return 0
      fi
    fi
    sleep 1
  done

  return 1
}

smoke_nyx() {
  require_cmd curl

  local port token binary_path base_url
  port="$(read_env_value "NYX_WEB_PORT")"
  token="$(read_env_value "NYX_API_TOKEN")"
  base_url="http://127.0.0.1:${port}"

  if [[ "${BUILD_MODE}" == "debug" ]]; then
    binary_path="${ROOT_DIR}/target/debug/nyx"
  else
    binary_path="${ROOT_DIR}/target/release/nyx"
  fi

  "${binary_path}" >"${ROOT_DIR}/workspace/first_run_local.log" 2>&1 &
  local server_pid=$!

  if wait_for_health "${base_url}" "${token}"; then
    echo
    echo "Nyx smoke run succeeded."
    echo "  web ui: ${base_url}"
    echo "  health: ${base_url}/health"
    echo "  operator brief: ${base_url}/api/operator/brief"
    echo "  bearer token: ${token}"
  else
    echo "Nyx failed to become healthy during smoke run." >&2
    echo "Log: ${ROOT_DIR}/workspace/first_run_local.log" >&2
    tail -80 "${ROOT_DIR}/workspace/first_run_local.log" >&2 || true
    kill "${server_pid}" >/dev/null 2>&1 || true
    wait "${server_pid}" >/dev/null 2>&1 || true
    exit 1
  fi

  kill "${server_pid}" >/dev/null 2>&1 || true
  wait "${server_pid}" >/dev/null 2>&1 || true
}

run_nyx() {
  local binary_path
  if [[ "${BUILD_MODE}" == "debug" ]]; then
    (
      cd "${ROOT_DIR}"
      cargo run --bin nyx
    )
  else
    binary_path="${ROOT_DIR}/target/release/nyx"
    exec "${binary_path}"
  fi
}

main() {
  mkdir -p "${ROOT_DIR}/workspace"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --smoke)
        MODE="smoke"
        shift
        ;;
      --run)
        MODE="run"
        shift
        ;;
      --debug)
        MODE="run"
        BUILD_MODE="debug"
        shift
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      *)
        echo "unknown argument: $1" >&2
        usage >&2
        exit 1
        ;;
    esac
  done

  prepare_first_run_env
  build_nyx

  case "${MODE}" in
    prepare)
      print_summary
      ;;
    smoke)
      smoke_nyx
      ;;
    run)
      print_summary
      run_nyx
      ;;
  esac
}

main "$@"
