#!/usr/bin/env bash
set -Eeuo pipefail

load_env_file() {
  local env_file="${1:-.env}"
  local mode mode_number group_bits other_bits
  local line_number=0
  local line key value

  [[ -f "$env_file" ]] || return 0
  [[ -r "$env_file" ]] || {
    printf 'Environment file is not readable: %s\n' "$env_file" >&2
    return 1
  }

  mode="$(stat -c '%a' -- "$env_file")"
  mode_number=$((10#$mode))
  group_bits=$(((mode_number / 10) % 10))
  other_bits=$((mode_number % 10))
  if (( group_bits != 0 || other_bits != 0 )); then
    printf 'Environment file must not be group/world accessible: %s (mode %s)\n' "$env_file" "$mode" >&2
    return 1
  fi

  while IFS= read -r line || [[ -n "$line" ]]; do
    line_number=$((line_number + 1))
    [[ "$line" =~ ^[[:space:]]*$ || "$line" =~ ^[[:space:]]*# ]] && continue

    if [[ ! "$line" =~ ^[[:space:]]*(export[[:space:]]+)?([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*=[[:space:]]*(.*)$ ]]; then
      printf 'Invalid environment line %s in %s\n' "$line_number" "$env_file" >&2
      return 1
    fi

    key="${BASH_REMATCH[2]}"
    value="${BASH_REMATCH[3]}"
    case "$key" in
      PATH|HOME|BASH_ENV|ENV|CDPATH|IFS|SHELLOPTS|BASHOPTS|LD_PRELOAD|LD_LIBRARY_PATH|PYTHONHOME|PYTHONPATH|PYTHONSTARTUP|NODE_OPTIONS|GIT_SSH_COMMAND|GIT_CONFIG_*|GIT_DIR|GIT_WORK_TREE|GIT_PAGER|GIT_EDITOR|PAGER|EDITOR|VISUAL|DOCKER_HOST|DOCKER_CONTEXT|DOCKER_CONFIG|DOCKER_CERT_PATH|DOCKER_TLS_VERIFY|COMPOSE_FILE|COMPOSE_PROJECT_NAME|COMPOSE_PROFILES|COMPOSE_PATH_SEPARATOR|PIP_CONFIG_FILE|NPM_CONFIG_*|npm_config_*|CURL_CA_BUNDLE|REQUESTS_CA_BUNDLE|HTTP_PROXY|HTTPS_PROXY|ALL_PROXY|NO_PROXY)
        printf 'Environment key is not allowed: %s\n' "$key" >&2
        return 1
        ;;
    esac
    if [[ "$value" == \"* ]]; then
      [[ "$value" == *\" && ${#value} -ge 2 ]] || {
        printf 'Unclosed double quote on line %s in %s\n' "$line_number" "$env_file" >&2
        return 1
      }
      value="${value:1:${#value}-2}"
    elif [[ "$value" == \'* ]]; then
      [[ "$value" == *\' && ${#value} -ge 2 ]] || {
        printf 'Unclosed single quote on line %s in %s\n' "$line_number" "$env_file" >&2
        return 1
      }
      value="${value:1:${#value}-2}"
    fi

    printf -v "$key" '%s' "$value"
    export "$key"
  done < "$env_file"
}