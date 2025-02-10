from fastapi import APIRouter, HTTPException
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from elastic_search.es import ElasticSearch

router = APIRouter()
es_client = ElasticSearch()

@router.get("/health", summary="Get Cluster Health")
def cluster_health():
    try:
        response = es_client.get_cluster_health()
        return {"status": "success", "health": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/stats", summary="Get Cluster Stats")
def cluster_stats():
    try:
        response = es_client.get_cluster_stats()
        return {"status": "success", "stats": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))