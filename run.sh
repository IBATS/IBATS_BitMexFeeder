#!/usr/bin/env bash
echo "run feeder"
cd ~/wspy/BitMexFeeder/
source venv/bin/activate
python3 run.py
