#!/usr/bin/env bash
pushd /home/ubuntu/cadre-data
source middleware/venv/bin/activate
exec python run_cadre_data.py
