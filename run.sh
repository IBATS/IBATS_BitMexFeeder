#!/usr/bin/env bash
echo "run feeder"
cd /home/mg/wspy/BitMexFeeder/
source venv/bin/activate
python3 run.py
