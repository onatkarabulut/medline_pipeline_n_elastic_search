from fastapi import APIRouter, HTTPException
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from fastapi import APIRouter, HTTPException
from elastic_search.es import ElasticSearch

router = APIRouter()
es_client = ElasticSearch()

@router.post("/scraping/start", summary="Trigger Scraping")
def trigger_scraping(end: int = -1):
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


# import threading
# import os
# import sys
# import time
# from fastapi import APIRouter, HTTPException
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
# from elastic_search.es import ElasticSearch

# router = APIRouter()
# es_client = ElasticSearch()
# stop_event = threading.Event()
# scraping_thread = None

# def scraping_task(end: int):
#     for _ in range(end or 10):
#         if stop_event.is_set():
#             break
#         es_client.trigger_scraping(end=1)
#         time.sleep(1)

# @router.post("/scraping/start", summary="Trigger Scraping")
# def scraping_start(end: int = None):
#     global scraping_thread, stop_event
#     if scraping_thread and scraping_thread.is_alive():
#         raise HTTPException(status_code=400, detail="Scraping is already running.")
#     stop_event.clear()
#     def run_scraping():
#         scraping_task(end=end)
#     scraping_thread = threading.Thread(target=run_scraping, daemon=True)
#     scraping_thread.start()
#     return {"status": "started", "end": end}

# @router.post("/scraping/cancel", summary="Cancel Scraping")
# def scraping_cancel():
#     global scraping_thread, stop_event
#     if not scraping_thread or not scraping_thread.is_alive():
#         return {"status": "idle", "detail": "No active scraping to cancel."}
#     stop_event.set()
#     return {"status": "cancelling", "detail": "Stop event set. Scraping will terminate shortly."}

# @router.get("/scraping/status", summary="Check Scraping Status")
# def scraping_status():
#     global scraping_thread
#     if scraping_thread and scraping_thread.is_alive():
#         return {"status": "running"}
#     return {"status": "idle"}

# @router.get("/kafka/status", summary="Get Kafka Status")
# def kafka_status():
#     try:
#         response = es_client.kafka_status()
#         return {"status": "success", "kafka_status": response}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
