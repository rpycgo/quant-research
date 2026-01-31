import json
import logging
from aiokafka import AIOKafkaProducer


logger = logging.getLogger("KafkaProducer")


class AsyncKafkaProducer:
    def __init__(self, bootstrap_servers, topic):
        """
        Wrapper class for AIOKafkaProducer to send JSON messages.
        
        Args:
            bootstrap_servers (str): Kafka broker address (e.g., 'localhost:9092').
            topic (str): Target Kafka topic name.
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None

    async def start(self):
        """
        Initialize and start the AIOKafkaProducer.
        """
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            await self.producer.start()
            logger.info(f"✅ Kafka Producer started. Target Topic: {self.topic}")
        except Exception as e:
            logger.critical(f"❌ Failed to start Kafka Producer: {e}")
            raise e

    async def stop(self):
        """
        Stop the producer and close connections.
        """
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka Producer stopped.")

    async def send(self, data):
        """
        Send data to the Kafka topic asynchronously.
        
        Args:
            data (dict): The data dictionary to send.
        """
        if not self.producer:
            logger.error("Producer is not initialized. Call start() first.")
            return

        try:
            # Send message and wait for the delivery report to ensure data integrity.
            # For extremely high throughput, we could remove 'await' (fire-and-forget),
            # but for financial data, safety is usually preferred.
            await self.producer.send_and_wait(self.topic, value=data)
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {e}")
