#!/bin/bash
set -e

ENV_NAME="medline_search"

echo "=========================================="
echo "Checking for Conda environment '$ENV_NAME'"
echo "=========================================="
if conda env list | grep -q "^${ENV_NAME}[[:space:]]"; then
    echo "Conda environment '$ENV_NAME' already exists."
else
    echo "Creating Conda environment '$ENV_NAME'"
    conda env create -f environment.yml
fi

echo "=========================================="
echo "Activating Conda environment '$ENV_NAME'"
echo "=========================================="
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate "$ENV_NAME"

echo "=========================================="
echo "Starting Docker Compose in detached mode"
echo "=========================================="
sudo docker-compose up > logs/docker.log 2>&1 &

echo "=========================================="
echo "Waiting for services to initialize... (Around 60 seconds)"
sleep 60

echo "=========================================="
echo "Creating Kafka topic: medline-drugs"
echo "=========================================="
sudo docker exec kafka-broker-1 kafka-topics --create \
  --topic medline-drugs \
  --partitions 2 \
  --replication-factor 2 \
  --bootstrap-server kafka-broker-1:9092

echo "=========================================="
echo "Listing Kafka topics"
echo "=========================================="
sudo docker exec kafka-broker-1 kafka-topics --list --bootstrap-server kafka-broker-1:9092

echo "=========================================="
echo "Starting FastAPI server"
echo "=========================================="
fastapi run api/app.py --reload > logs/api_logs/api.log 2>&1 &

echo "=========================================="
echo "Starting Kafka consumer"
echo "=========================================="
python3 pipeline/consumer.py > logs/pipeline_logs/consumer.log 2>&1 &

echo "=========================================="
echo "All processes started. Enjoy!"
echo "=========================================="
