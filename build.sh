#!/usr/bin/env bash
# exit on error
set -o errexit

pip install -r requirements.txt

# Install Playwright chromium browser binaries if playwright is installed
if python3 -c "import playwright" &> /dev/null; then
    echo "Installing Playwright Chromium dependencies..."
    playwright install --with-deps chromium
else
    echo "Playwright not installed, skipping browser install."
fi
