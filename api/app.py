from fastapi import FastAPI
from routers import index, document, search, cluster, extra

app = FastAPI(
    title="Medline Elasticsearch API",
    description=(
        "A modular FastAPI application that integrates advanced Elasticsearch features "
        "for Medline drug data. This API wraps core Elasticsearch operations (document CRUD, "
        "search, aggregation, cluster management) along with project-specific endpoints for "
        "triggering scraping and checking Kafka status."
    ),
    version="1.0.0"

)

app.include_router(index.router, prefix="/index", tags=["Index"])
app.include_router(document.router, prefix="/document", tags=["Document"])
app.include_router(search.router, prefix="/search", tags=["Search"])
app.include_router(cluster.router, prefix="/cluster", tags=["Cluster"])
app.include_router(extra.router, prefix="/extra", tags=["Extra"])
