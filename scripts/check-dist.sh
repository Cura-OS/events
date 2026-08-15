#!/usr/bin/env bash
set -euo pipefail

root=$PWD
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cp -R src tsconfig.json "$tmp"
ln -s "$root/../../../node_modules" "$tmp/node_modules"
(
  cd "$tmp"
  bunx tsc
)
diff -ru "$tmp/dist" dist
