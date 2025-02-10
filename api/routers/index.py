from fastapi import APIRouter, HTTPException
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from api.models.es_models import IndexRequest
from elastic_search.es import ElasticSearch

router = APIRouter()
es_client = ElasticSearch()

@router.get("/list", summary="List All Indices")
def list_indices():
    try:
        indices = es_client.list_indices()
        return {"status": "success", "indices": indices}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/stats/{index_name}", summary="Get Index Statistics")
def get_index_stats(index_name: str):
    try:
        stats = es_client.get_index_stats(index_name)
        return {"status": "success", "stats": stats}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/refresh", summary="Refresh an Index")
def refresh_index(index_name: str):
    try:
        response = es_client.refresh_index(index_name)
        return {"status": "success", "response": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/open", summary="Open an Index")
def open_index(index_name: str):
    try:
        response = es_client.open_index(index_name)
        return {"status": "success", "response": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/close", summary="Close an Index")
def close_index(index_name: str):
    try:
        response = es_client.close_index(index_name)
        return {"status": "success", "response": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/create", summary="Create an Index")
def create_index(request: IndexRequest):
    try:
        response = es_client.create_index(request.index_name, mapping=request.mapping, settings=request.settings)
        if response is None:
            return {"status": "warning", "message": f"Index '{request.index_name}' already exists."}
        return {"status": "success", "response": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{index_name}", summary="Delete an Index")
def delete_index(index_name: str):
    try:
        response = es_client.delete_index(index_name)
        if response is None:
            return {"status": "warning", "message": f"Index '{index_name}' does not exist."}
        return {"status": "success", "response": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))