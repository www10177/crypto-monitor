#!/bin/bash
cd "$(dirname "$0")"
./.venv/bin/python3 main.py >> ./log-sh.log 2>&1
