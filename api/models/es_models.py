from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List, Union

class IndexRequest(BaseModel):
    index_name: str
    mapping: Optional[Dict[str, Any]] = None
    settings: Optional[Dict[str, Any]] = None

class DocumentRequest(BaseModel):
    index_name: str
    document: Dict[str, Any]
    doc_id: Optional[str] = None

class UpdateDocumentRequest(BaseModel):
    index_name: str
    doc_id: str
    update_body: Dict[str, Any]

class SearchRequest(BaseModel):
    index_name: str
    query: Dict[str, Any]
    size: Optional[int] = 10
    from_: Optional[int] = Field(0, alias="from")

class BulkIndexAction(BaseModel):
    index_name: str
    document: Dict[str, Any]
    doc_id: Optional[str] = None

class BulkIndexRequest(BaseModel):
    actions: List[BulkIndexAction]

class ReindexRequest(BaseModel):
    source_index: str
    dest_index: str
    query: Optional[Dict[str, Any]] = None

class SuggestRequest(BaseModel):
    index_name: str
    suggest_body: Dict[str, Any]

class UpdateByQueryRequest(BaseModel):
    index_name: str
    query: Dict[str, Any]
    script: str
    params: Optional[Dict[str, Any]] = None

class DeleteByQueryRequest(BaseModel):
    index_name: str
    query: Dict[str, Any]

class MultiGetRequest(BaseModel):
    index_name: str
    doc_ids: List[Union[str, int]]

class ExtendedSearch(BaseModel):
    query: str
    top_n: int