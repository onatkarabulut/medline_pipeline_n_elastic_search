import json
from fastapi import APIRouter, HTTPException, Query
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from api.models.es_models import ExtendedSearch, SearchRequest,SuggestRequest
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from elastic_search.es import ElasticSearch
from elastic_search.search_cli import ElasticSearchQuery

router = APIRouter()
es_client = ElasticSearch()

@router.post("/", summary="Search Documents")
def search(request: SearchRequest):
    try:
        response = es_client.search(request.index_name, query=request.query, size=request.size, from_=request.from_)
        return {"status": "success", "results": response.get("hits", {}).get("hits", [])}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/scroll", summary="Scroll Search")
def scroll_search(request: SearchRequest):
    try:
        results = es_client.scroll_search(request.index_name, query=request.query, size=request.size)
        return {"status": "success", "results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/aggregate", summary="Aggregate Data")
def aggregate(index_name: str, aggregation: dict):
    try:
        response = es_client.aggregate(index_name, aggregation)
        return {"status": "success", "response": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/highlight", summary="Search with Highlight")
def search_highlight(request: SearchRequest, highlight: dict):
    try:
        response = es_client.search_highlight(request.index_name, query=request.query, highlight=highlight, size=request.size, from_=request.from_)
        return {"status": "success", "results": response.get("hits", {}).get("hits", [])}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/suggest", summary="Suggest Queries")
def suggest(request: SuggestRequest):
    try:
        response = es_client.suggest(request.index_name, request.suggest_body)
        suggestions = response.get("suggest", {})
        return {"status": "success", "suggestions": suggestions}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/cli", summary="Search Cli From /elastic_search/search_cli.py")
def search_extended(
    query: str = Query(..., description="Search query string. Example: 'Pentamidine injection'"),
    top_n: int = Query(3, description="Number of top results to return")
):
    if not query.strip():
        raise HTTPException(
            status_code=400,
            detail="Query must not be empty. Example usage: /search/fuzzy?query=Pentamidine injection&top_n=10"
        )
    try:
        es_query = ElasticSearchQuery(host="localhost", port=9200, index_name="drug_data")
        results = es_query.fuzzy_search(query, top_n)
        return {"status": "success", "results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))