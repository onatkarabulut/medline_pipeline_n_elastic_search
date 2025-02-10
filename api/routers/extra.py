from fastapi import APIRouter, HTTPException
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from fastapi import APIRouter, HTTPException
from elastic_search.es import ElasticSearch

router = APIRouter()
es_client = ElasticSearch()

@router.post("/scraping/start", summary="Trigger Scraping")
def trigger_scraping(end: int = None):
    try:
        response = es_client.trigger_scraping(end=end)
        return {"status": "success", "output": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/kafka/status", summary="Get Kafka Status")
def kafka_status():
    try:
        response = es_client.kafka_status()
        return {"status": "success", "kafka_status": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))