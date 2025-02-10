import subprocess
import logging
import os
import sys
import json
import time
from typing import Optional, Dict, Any, List, Union
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ElasticsearchException
from confluent_kafka import Consumer, TopicPartition

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class ElasticSearch:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 9200,
    ):
        self.ES_HOST = host
        self.ES_PORT = port
        scheme = "http"
        try:
            self.client = Elasticsearch(
                hosts=[{"host": self.ES_HOST, "port": self.ES_PORT, "scheme": scheme}]
            )
            if self.client.ping():
                logger.info("Successfully connected to Elasticsearch.")
            else:
                logger.warning("Could not connect to Elasticsearch.")
        except Exception as e:
            logger.error(f"Error creating ES client: {e}")
            raise e

    def create_index(self, index_name: str, mapping: Optional[Dict[str, Any]] = None, settings: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        body: Dict[str, Any] = {}
        if settings:
            body['settings'] = settings
        if mapping:
            body['mappings'] = mapping
        try:
            if self.client.indices.exists(index=index_name):
                logger.warning(f"Index '{index_name}' already exists.")
                return None
            else:
                response = self.client.indices.create(index=index_name, body=body)
                logger.info(f"Index '{index_name}' created: {response}")
                return response
        except ElasticsearchException as e:
            logger.error(f"Error creating index '{index_name}': {e}")
            raise e

    def delete_index(self, index_name: str) -> Optional[Any]:
        try:
            if self.client.indices.exists(index=index_name):
                response = self.client.indices.delete(index=index_name)
                logger.info(f"Index '{index_name}' deleted: {response}")
                return response
            else:
                logger.warning(f"Index '{index_name}' does not exist.")
                return None
        except ElasticsearchException as e:
            logger.error(f"Error deleting index '{index_name}': {e}")
            raise e

    def index_document(self, index_name: str, document: Dict[str, Any], doc_id: Optional[Union[str, int]] = None) -> Any:
        try:
            doc_id_str = str(doc_id) if doc_id is not None else None
            response = self.client.index(index=index_name, id=doc_id_str, body=document)
            logger.info(f"Document indexed: {response}")
            return response
        except ElasticsearchException as e:
            logger.error(f"Error indexing document: {e}")
            raise e

    def get_document(self, index_name: str, doc_id: Union[str, int]) -> Any:
        try:
            doc_id_str = str(doc_id)
            response = self.client.get(index=index_name, id=doc_id_str)
            logger.info(f"Document retrieved: {response}")
            return response
        except ElasticsearchException as e:
            logger.error(f"Error retrieving document: {e}")
            raise e

    def update_document(self, index_name: str, doc_id: Union[str, int], update_body: Dict[str, Any]) -> Any:
        try:
            doc_id_str = str(doc_id)
            response = self.client.update(index=index_name, id=doc_id_str, body=update_body)
            logger.info(f"Document updated: {response}")
            return response
        except ElasticsearchException as e:
            logger.error(f"Error updating document: {e}")
            raise e

    def delete_document(self, index_name: str, doc_id: Union[str, int]) -> Any:
        try:
            doc_id_str = str(doc_id)
            response = self.client.delete(index=index_name, id=doc_id_str)
            logger.info(f"Document deleted: {response}")
            return response
        except ElasticsearchException as e:
            logger.error(f"Error deleting document: {e}")
            raise e

    def search(self, index_name: str, query: Dict[str, Any], size: int = 10, from_: int = 0) -> Any:
        try:
            response = self.client.search(index=index_name, body=query, size=size, from_=from_)
            logger.info(f"Search performed: {response}")
            return response
        except ElasticsearchException as e:
            logger.error(f"Error performing search: {e}")
            raise e

    def bulk_index(self, actions: List[Dict[str, Any]]) -> Any:
        try:
            response = helpers.bulk(self.client, actions)
            logger.info(f"Bulk indexing completed: {response}")
            return response
        except ElasticsearchException as e:
            logger.error(f"Error during bulk indexing: {e}")
            raise e

    def scroll_search(self, index_name: str, query: Dict[str, Any], scroll: str = "2m", size: int = 1000) -> List[Any]:
        try:
            all_hits: List[Any] = []
            response = self.client.search(index=index_name, body=query, scroll=scroll, size=size)
            scroll_id = response.get('_scroll_id')
            hits = response['hits']['hits']
            all_hits.extend(hits)
            while True:
                response = self.client.scroll(scroll_id=scroll_id, scroll=scroll)
                scroll_id = response.get('_scroll_id')
                hits = response['hits']['hits']
                if not hits:
                    break
                all_hits.extend(hits)
            self.client.clear_scroll(scroll_id=scroll_id)
            logger.info(f"Scroll search completed, retrieved {len(all_hits)} documents.")
            return all_hits
        except ElasticsearchException as e:
            logger.error(f"Error during scroll search: {e}")
            raise e

    def aggregate(self, index_name: str, aggregation: Dict[str, Any]) -> Any:
        query = {
            "size": 0,
            "aggs": aggregation
        }
        try:
            response = self.client.search(index=index_name, body=query)
            logger.info(f"Aggregation performed: {response}")
            return response
        except ElasticsearchException as e:
            logger.error(f"Error performing aggregation: {e}")
            raise e

    def reindex(self, source_index: str, dest_index: str, query: Optional[Dict[str, Any]] = None) -> Any:
        body: Dict[str, Any] = {}
        if query:
            body["query"] = query
        try:
            response = self.client.reindex(
                body=body,
                source={"index": source_index},
                dest={"index": dest_index}
            )
            logger.info(f"Reindexing from '{source_index}' to '{dest_index}' completed: {response}")
            return response
        except ElasticsearchException as e:
            logger.error(f"Error during reindexing: {e}")
            raise e

    def get_cluster_health(self) -> Any:
        try:
            response = self.client.cluster.health()
            logger.info(f"Cluster health: {response}")
            return response
        except ElasticsearchException as e:
            logger.error(f"Error getting cluster health: {e}")
            raise e

    def get_cluster_stats(self) -> Any:
        try:
            response = self.client.cluster.stats()
            logger.info(f"Cluster stats: {response}")
            return response
        except ElasticsearchException as e:
            logger.error(f"Error getting cluster stats: {e}")
            raise e

    def get_index_stats(self, index_name: str) -> Any:
        try:
            response = self.client.indices.stats(index=index_name)
            logger.info(f"Index stats for {index_name}: {response}")
            return response
        except ElasticsearchException as e:
            logger.error(f"Error getting index stats for {index_name}: {e}")
            raise e

    def count_documents(self, index_name: str, query: Optional[Dict[str, Any]] = None) -> int:
        try:
            if query is None:
                response = self.client.count(index=index_name)
            else:
                response = self.client.count(index=index_name, body={"query": query})
            count = response.get("count", 0)
            logger.info(f"Document count in '{index_name}': {count}")
            return count
        except ElasticsearchException as e:
            logger.error(f"Error counting documents in '{index_name}': {e}")
            raise e

    def get_latest_document(self, index_name: str) -> Any:
        try:
            query = {
                "size": 1,
                "sort": [{"scraped_at": {"order": "desc"}}]
            }
            response = self.client.search(index=index_name, body=query)
            hits = response.get("hits", {}).get("hits", [])
            if hits:
                logger.info(f"Latest document from '{index_name}': {hits[0]}")
                return hits[0]
            else:
                logger.info(f"No documents found in index '{index_name}'")
                return None
        except ElasticsearchException as e:
            logger.error(f"Error retrieving latest document from '{index_name}': {e}")
            raise e

    def trigger_scraping(self, end: Optional[int] = None) -> dict:
        try:
            cmd = ["python3", "scraping/scraper.py"]
            if end is not None:
                cmd.extend(["--end", str(end)])
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.info("Scraping triggered successfully.")
            return {"status": "success", "output": result.stdout}
        except subprocess.CalledProcessError as e:
            logger.error(f"Error triggering scraping: {e.stderr}")
            raise Exception(f"Scraping failed: {e.stderr}")

    # def kafka_status(self) -> dict:
    #     try:
    #         status = {
    #             "broker_status": "running",
    #             "topics": ["medline-drugs"],
    #             "consumer_group": "python-consumer-group",
    #             "message_rate": "approx. 100 msg/min"  # Example data
    #         }
    #         logger.info(f"Kafka status: {status}")
    #         return status
    #     except Exception as e:
    #         logger.error(f"Error getting Kafka status: {e}")
    #         raise e

    def get_message_rate(self, topic: str) -> int:
        conf = {
            'bootstrap.servers': 'localhost:19092,localhost:29092',
            'group.id': 'dummy-group-for-offsets',
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(conf)
        md = consumer.list_topics(topic, timeout=10)
        total = 0
        for p in md.topics[topic].partitions.keys():
            tp = TopicPartition(topic, p)
            low, high = consumer.get_watermark_offsets(tp, timeout=10)
            total += (high - low)
        consumer.close()
        return total

    def kafka_status(self) -> dict:
        try:
            initial_count = self.get_message_rate("medline-drugs")
            import time
            time.sleep(1)
            new_count = self.get_message_rate("medline-drugs")
            delta = new_count - initial_count
            message_rate = delta * 60

            conf = {
                'bootstrap.servers': 'localhost:19092,localhost:29092',
                'group.id': 'dummy-group-for-topics',
                'auto.offset.reset': 'earliest'
            }
            consumer = Consumer(conf)
            metadata = consumer.list_topics(timeout=10)
            topics = list(metadata.topics.keys())
            consumer.close()

            status = {
                "broker_status": "running",
                "topics": topics,
                "consumer_group": "python-consumer-group",
                "message_rate": f"{message_rate:.2f} msg/min"
            }
            logger.info(f"Kafka status: {status}")
            return status
        except Exception as e:
            logger.error(f"Error getting Kafka status: {e}")
            raise e


    def search_highlight(self, index_name: str, query: Dict[str, Any], highlight: Dict[str, Any], size: int = 10, from_: int = 0) -> Any:
        body = query.copy()
        body["highlight"] = highlight
        try:
            response = self.client.search(index=index_name, body=body, size=size, from_=from_)
            logger.info(f"Search with highlight performed: {response}")
            return response
        except ElasticsearchException as e:
            logger.error(f"Error performing search with highlight: {e}")
            raise e

    def explain_query(self, index_name: str, doc_id: Union[str, int], query: Dict[str, Any]) -> Any:
        try:
            doc_id_str = str(doc_id)
            response = self.client.explain(index=index_name, id=doc_id_str, body={"query": query})
            logger.info(f"Explain query performed: {response}")
            return response
        except ElasticsearchException as e:
            logger.error(f"Error explaining query: {e}")
            raise e

    def update_by_query(self, index_name: str, query: Dict[str, Any], script: str, params: Optional[Dict[str, Any]] = None) -> Any:
        body = {
            "query": query,
            "script": {"source": script, "params": params or {}}
        }
        try:
            response = self.client.update_by_query(index=index_name, body=body)
            logger.info(f"Update by query performed: {response}")
            return response
        except ElasticsearchException as e:
            logger.error(f"Error performing update by query: {e}")
            raise e

    def delete_by_query(self, index_name: str, query: Dict[str, Any]) -> Any:
        body = {"query": query}
        try:
            response = self.client.delete_by_query(index=index_name, body=body)
            logger.info(f"Delete by query performed: {response}")
            return response
        except ElasticsearchException as e:
            logger.error(f"Error performing delete by query: {e}")
            raise e

    def multi_get(self, index_name: str, doc_ids: List[Union[str, int]]) -> Any:
        docs = [{"_index": index_name, "_id": str(doc_id)} for doc_id in doc_ids]
        try:
            response = self.client.mget(body={"docs": docs})
            logger.info(f"Multi-get performed: {response}")
            return response
        except ElasticsearchException as e:
            logger.error(f"Error performing multi_get: {e}")
            raise e

    def list_indices(self) -> List[str]:
        try:
            response = self.client.cat.indices(format="json")
            indices = [item["index"] for item in response]
            logger.info(f"Indices listed: {indices}")
            return indices
        except ElasticsearchException as e:
            logger.error(f"Error listing indices: {e}")

            raise e
    def suggest(self, index_name: str, suggest_body: Dict[str, Any]) -> Any:
        try:
            response = self.client.search(index=index_name, body={"suggest": suggest_body})
            logger.info(f"Suggest query performed: {response}")
            return response
        except ElasticsearchException as e:
            logger.error(f"Error performing suggest query: {e}")
            raise e
            
    def refresh_index(self, index_name: str) -> Any:
        try:
            response = self.client.indices.refresh(index=index_name)
            logger.info(f"Index '{index_name}' refreshed: {response}")
            return response
        except ElasticsearchException as e:
            logger.error(f"Error refreshing index '{index_name}': {e}")
            raise e

    def open_index(self, index_name: str) -> Any:
        try:
            response = self.client.indices.open(index=index_name)
            logger.info(f"Index '{index_name}' opened: {response}")
            return response
        except ElasticsearchException as e:
            logger.error(f"Error opening index '{index_name}': {e}")
            raise e

    def close_index(self, index_name: str) -> Any:
        try:
            response = self.client.indices.close(index=index_name)
            logger.info(f"Index '{index_name}' closed: {response}")
            return response
        except ElasticsearchException as e:
            logger.error(f"Error closing index '{index_name}': {e}")
            raise e