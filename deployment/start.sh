#!/usr/bin/env bash
pushd /home/ubuntu/cadre-data
source venv/bin/activate
exec python run_cadre_data.py
