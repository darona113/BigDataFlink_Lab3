#!/bin/bash
set -e

docker compose exec jobmanager flink run -py /opt/flink/jobs/stream_to_star.py
