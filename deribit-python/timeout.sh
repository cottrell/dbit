#!/bin/bash
# run
# Run command with timeout $1 seconds.

# Timeout seconds
timeout_seconds="$1"
shift

# PID
pid=$$

# Start timeout
(
  sleep "$timeout_seconds"
  echo "Timed out after $timeout_seconds seconds"
  kill -- -$pid &>/dev/null
) &
timeout_pid=$!

# Run
"$@"

# Stop timeout
kill $timeout_pid &>/dev/null
