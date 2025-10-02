#!/bin/bash
set -e

# Decode cookies from secret
if [ -n "$YT_COOKIES_B64" ]; then
  echo "$YT_COOKIES_B64" | base64 -d > /tmp/cookies.txt
  export COOKIES_FILE=/tmp/cookies.txt
  echo "[fly-entrypoint] Cookies written to /tmp/cookies.txt"
else
  echo "[fly-entrypoint] Warning: No cookies secret provided"
fi

# Run your worker
exec python run_worker.py
