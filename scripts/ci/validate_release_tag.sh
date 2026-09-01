#!/usr/bin/env bash
set -euo pipefail

tag="${1:-}"
if [[ ! "${tag}" =~ ^mutagen-v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "invalid immutable Mutagen engine release tag: ${tag:-<empty>}" >&2
  exit 1
fi
