from confluent_kafka import Producer
import logging
import time

logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class KafkaProducer:
    def __init__(self):
        self.conf = {
            'bootstrap.servers': 'localhost:19092,localhost:29092',
            'client.id': 'python-producer',
            'acks': 'all'
        }
        self.test = "OK"
        self.producer = Producer(self.conf)
        logger.info("Producer initialized with config: %s", self.conf)

    def delivery_report(self, err, msg):
        if err:
            logger.error("Message delivery failed: %s", err)
        else:
            logger.info(
                "Message delivered to %s [%s] (offset: %d)",
                msg.topic(), msg.partition(), msg.offset()
            )
        pass

    def produce(self, topic, value):
        try:
            self.producer.produce(
                topic=topic,
                value=value.encode('utf-8'),
                callback=self.delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error("Production error: %s", str(e))
            raise

    def flush(self):
        self.producer.flush()
        logger.info("Producer flush completed")

