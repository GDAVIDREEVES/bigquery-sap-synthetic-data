#!/usr/bin/env bash
# Run after adding ~/.ssh/id_ed25519.pub to GitHub (Settings → SSH keys).
set -euo pipefail
cd "$(dirname "$0")/.."
echo "Testing SSH to GitHub..."
ssh -T git@github.com
echo "Pushing to origin..."
git push -u origin main
