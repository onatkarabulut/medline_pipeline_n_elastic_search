from fastapi import APIRouter, HTTPException
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from api.models.es_models import DocumentRequest, UpdateDocumentRequest, UpdateByQueryRequest, DeleteByQueryRequest, MultiGetRequest
from elastic_search.es import ElasticSearch

router = APIRouter()
es_client = ElasticSearch()

@router.post("/", summary="Index a Document")
def index_document(request: DocumentRequest):
    try:
        response = es_client.index_document(request.index_name, request.document, doc_id=request.doc_id)
        return {"status": "success", "response": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{index_name}/{doc_id}", summary="Get a Document")
def get_document(index_name: str, doc_id: str):
    try:
        response = es_client.get_document(index_name, doc_id)
        return {"status": "success", "document": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/{index_name}/{doc_id}", summary="Update a Document")
def update_document(index_name: str, doc_id: str, request: UpdateDocumentRequest):
    try:
        response = es_client.update_document(index_name, doc_id, request.update_body)
        return {"status": "success", "response": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{index_name}/{doc_id}", summary="Delete a Document")
def delete_document(index_name: str, doc_id: str):
    try:
        response = es_client.delete_document(index_name, doc_id)
        return {"status": "success", "response": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/update-by-query", summary="Update Documents by Query")
def update_by_query(request: UpdateByQueryRequest):
    try:
        response = es_client.update_by_query(request.index_name, request.query, request.script, request.params)
        return {"status": "success", "response": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/delete-by-query", summary="Delete Documents by Query")
def delete_by_query(request: DeleteByQueryRequest):
    try:
        response = es_client.delete_by_query(request.index_name, request.query)
        return {"status": "success", "response": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/multi-get", summary="Multi-get Documents")
def multi_get(request: MultiGetRequest):
    try:
        response = es_client.multi_get(request.index_name, request.doc_ids)
        return {"status": "success", "response": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{index_name}/{doc_id}/explain", summary="Explain Document Scoring")
def explain_query(index_name: str, doc_id: str, query: dict):
    try:
        response = es_client.explain_query(index_name, doc_id, query)
        return {"status": "success", "explanation": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
