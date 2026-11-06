#!/usr/bin/env bash
# Poll Herdr agents every 2 minutes. When an agent's screen shows a test and
# that screen stays unchanged for a long stretch, tell the agent to background
# the test and keep working.
set -u

INTERVAL="${HERDR_WATCH_INTERVAL:-120}"
STALL_SECONDS="${HERDR_WATCH_STALL_SECONDS:-600}"
STATE_DIR="${HERDR_WATCH_STATE:-${XDG_STATE_HOME:-$HOME/.local/state}/dbos-herdr-watch}"
LOG="$STATE_DIR/watch.log"

if [[ "${HERDR_ENV:-}" != 1 ]]; then
  echo "herdr-agent-watch: HERDR_ENV is not 1. Run this inside a Herdr pane." >&2
  exit 1
fi

mkdir -p "$STATE_DIR"
exec 9>>"$STATE_DIR/watch.lock"
if ! flock -n 9; then
  echo "herdr-agent-watch: already running" >&2
  exit 1
fi

log() {
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "$LOG"
}

screen_of() {
  herdr agent read "$1" --source recent-unwrapped --lines 80 2>/dev/null || true
}

test_still_running() {
  local screen="$1"
  local last
  last="$(printf '%s\n' "$screen" | tail -n 25)"
  if printf '%s\n' "$last" | grep -E -q 'test result:|passed in |failed in |error: test failed|[0-9]+ passed'; then
    return 1
  fi
  printf '%s\n' "$screen" | grep -E -q 'cargo test|pytest|nextest|cargo nextest|Running [0-9]+ tests|running tests'
}

nudge() {
  local target="$1"
  herdr agent prompt "$target" "Your screen has shown a test with no new output for over 10 minutes. Stop waiting on it. Run that test in the background, and keep working on the next decision."
}

# Drop hashes from panes that have since been closed.
find "$STATE_DIR" -maxdepth 1 -type f -name 'w*' -delete 2>/dev/null || true

log "watching every ${INTERVAL}s, stall ${STALL_SECONDS}s, workspace ${HERDR_WORKSPACE_ID:-none}, skipping pane ${HERDR_PANE_ID:-none}"

while true; do
  now=$(date +%s)
  if ! list=$(herdr agent list 2>>"$LOG"); then
    log "agent list failed"
    sleep "$INTERVAL"
    continue
  fi

  summary=""
  while IFS=$'\t' read -r pane name kind status title; do
    [[ -n "$pane" ]] || continue
    if [[ "$pane" == "${HERDR_PANE_ID:-}" || "$kind" == "cursor" ]]; then
      continue
    fi
    label="${name:-$pane}"
    # idle and done have finished their turn. Counting them made the watch list lie
    # after agents were closed or had already reported.
    if [[ "$status" != "working" && "$status" != "unknown" ]]; then
      continue
    fi

    screen="$(screen_of "$pane")"
    hash=$(printf '%s' "$screen" | sha256sum | awk '{print $1}')
    safe=${pane//\//_}
    file="$STATE_DIR/$safe"
    prev_hash=""
    changed_at=$now
    nudged_at=0
    if [[ -f "$file" ]]; then
      {
        read -r prev_hash
        read -r changed_at
        read -r nudged_at
      } <"$file"
    fi

    note="moved"
    if [[ "$hash" == "$prev_hash" ]]; then
      idle_for=$((now - changed_at))
      since_nudge=$((now - nudged_at))
      note="unchanged ${idle_for}s"
      if (( idle_for >= STALL_SECONDS && since_nudge >= STALL_SECONDS )) && test_still_running "$screen"; then
        log "stuck test on $label ($kind $status ${title:-}) for ${idle_for}s"
        if nudge "$pane" >>"$LOG" 2>&1; then
          nudged_at=$now
          note="nudged"
          log "told $label to background the test"
        else
          log "could not prompt $label (status $status)"
        fi
      fi
    else
      changed_at=$now
    fi
    summary="${summary}${label}=${status}/${note} "

    printf '%s\n%s\n%s\n' "$hash" "$changed_at" "${nudged_at:-0}" >"$file"
  done < <(printf '%s' "$list" | jq -r --arg ws "${HERDR_WORKSPACE_ID:-}" '.result.agents[] | select(.workspace_id == $ws) | [.pane_id, (.name // ""), .agent, .agent_status, (.terminal_title_stripped // "")] | @tsv')

  log "poll ${summary:-no working agents}"
  sleep "$INTERVAL"
done
