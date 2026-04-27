#!/bin/bash
# Запуск всех трёх сервисов TraderBot в фоне
set -e

cd "$(dirname "$0")/.."

mkdir -p logs

echo "Starting TraderBot services..."

python -m analyst.main &
echo "ANALYST pid=$!"

python -m executor.main &
echo "EXECUTOR pid=$!"

python -m risk_manager.main &
echo "RISK_MANAGER pid=$!"

echo "All services started. Logs in ./logs/"
echo "Stop with: kill \$(cat logs/*.pid) or pkill -f traderbot"

wait
