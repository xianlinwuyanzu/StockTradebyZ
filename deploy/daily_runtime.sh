#!/usr/bin/env bash

init_daily_runtime() {
  DAILY_STARTED_SECONDS=$SECONDS
  trap 'daily_exit_code=$?; printf "[timing] stage=total elapsed_seconds=%s exit_code=%s\n" "$((SECONDS - DAILY_STARTED_SECONDS))" "$daily_exit_code"' EXIT
  SELECT_TIMING_OUTPUT="${SELECT_TIMING_OUTPUT:-./logs/selection-timing-$(date +%Y%m%d-%H%M%S)-$$.json}"
  SELECTION_EXTRA_ARGS=(--timing-output "$SELECT_TIMING_OUTPUT")
  if [[ "${SELECTION_VISUALIZATION_ENABLED:-1}" == "0" ]]; then
    SELECTION_EXTRA_ARGS+=(--no-visualization)
  fi
  if [[ "${SELECTION_DASHBOARD_ENABLED:-1}" == "0" ]]; then
    SELECTION_EXTRA_ARGS+=(--no-dashboard)
  fi
}

run_timed_stage() {
  local stage="$1"
  shift
  local started=$SECONDS
  local exit_code=0
  if "$@"; then
    exit_code=0
  else
    exit_code=$?
  fi
  printf '[timing] stage=%s elapsed_seconds=%s exit_code=%s\n' "$stage" "$((SECONDS - started))" "$exit_code"
  return "$exit_code"
}