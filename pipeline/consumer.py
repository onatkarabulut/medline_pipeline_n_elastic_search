cd import logging
import json
import hashlib
import re
import time
from confluent_kafka import Consumer, KafkaError, KafkaException
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from elastic_search.es import ElasticSearch

logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def slugify(text: str) -> str:
    return re.sub(r'[^a-z0-9]+', '_', text.lower()).strip('_')

def generate_doc_id_from_url(url: str) -> str:
    doc_id = url.strip().lower().replace("https://", "").replace("http://", "")
    doc_id = re.sub(r'[\/\.]', '_', doc_id)
    return doc_id

def flatten_medication_record(record: dict) -> dict:
    flattened = {
        "name": record.get("name"),
        "url": record.get("url")
    }
    sections = record.get("sections", [])
    if isinstance(sections, list):
        for section in sections:
            if section and isinstance(section, dict):
                title = section.get("title", "").strip()
                content = section.get("content", "").strip()
                if title and content:
                    field_name = slugify(title)
                    flattened[field_name] = content
    return flattened

class KafkaConsumer:
    def __init__(self):
        self.conf = {
            'bootstrap.servers': 'localhost:19092,localhost:29092',
            'group.id': 'python-consumer-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'session.timeout.ms': 6000,
            'max.poll.interval.ms': 6000
        }
        self.consumer = Consumer(self.conf)
        logger.info("Consumer initialized with config: %s", self.conf)
        
        self.es_client = ElasticSearch()
        self.es_index = "drug_data"

    def subscribe(self, topics: list):
        self.consumer.subscribe(topics)
        logger.info("Subscribed to topics: %s", topics)

    def consume(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.warning("Reached end of partition for topic %s", msg.topic())
                    else:
                        raise KafkaException(msg.error())
                else:
                    self.process_message(msg)
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user.")
        finally:
            self.close()

    def process_message(self, msg):
        try:
            message_str = msg.value().decode('utf-8')
            logger.info(
                "Received message (topic: %s, partition: %d, offset: %d): %s",
                msg.topic(), msg.partition(), msg.offset(), message_str
            )
            data = json.loads(message_str)

            if isinstance(data, list):
                for item in data:
                    flattened = flatten_medication_record(item)
                    if "name" in item and item["name"]:
                        doc_id = slugify(item["name"])
                    else:
                        doc_id = generate_doc_id_from_url(item.get("url", ""))
                    self.es_client.index_document(self.es_index, flattened, doc_id=doc_id)
                    logger.info("Indexed document with doc_id: %s", doc_id)
            elif isinstance(data, dict):
                flattened = flatten_medication_record(data)
                if "name" in data and data["name"]:
                    doc_id = slugify(data["name"])
                else:
                    doc_id = generate_doc_id_from_url(data.get("url", ""))
                self.es_client.index_document(self.es_index, flattened, doc_id=doc_id)
                logger.info("Indexed document with doc_id: %s", doc_id)
            else:
                logger.warning("Unexpected data type received: %s", type(data))
            
            self.consumer.commit(msg)
            logger.info("Message processed and committed.")
        except Exception as e:
            logger.error("Error processing message: %s", e)

    def close(self):
        self.consumer.close()
        logger.info("Consumer closed.")

if __name__ == "__main__":
    consumer = KafkaConsumer()
    consumer.subscribe(["medline-drugs"])
    consumer.consume()
