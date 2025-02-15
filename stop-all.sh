#!/bin/bash
set -e

# Ortak: Belirtilen desenle çalışan işlemi durdurur.
kill_process() {
    local pattern="$1"
    echo "Attempting to kill process matching pattern: \"$pattern\""
    if pkill -f "$pattern"; then
        echo "Process matching \"$pattern\" killed."
    else
        echo "No process found matching \"$pattern\"."
    fi
}

echo "=========================================="
echo "Stopping Docker Compose services"
echo "=========================================="
sudo docker-compose down

echo "=========================================="
echo "Stopping FastAPI server"
echo "=========================================="
kill_process "fastapi run api/app.py"

echo "=========================================="
echo "Stopping Kafka consumer process"
echo "=========================================="
kill_process "python3 pipeline/consumer.py"

echo "=========================================="
echo "Stopping Scraper process (if running)"
echo "=========================================="
kill_process "python3 dags/scraper_DAG.py"

echo "=========================================="
echo "All processes stopped."
echo "=========================================="
