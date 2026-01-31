import asyncio
import json
import logging
import sys
from aiokafka import AIOKafkaConsumer

from src.utils.config_loader import ConfigManager
from src.ingestor.database_handler import TimescaleIngestor


# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [INGESTOR] - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger("IngestionWorker")


class DataIngestionService:
    def __init__(self):
        # Load Config
        self.config_manager = ConfigManager()
        self.kafka_cfg = self.config_manager.get_kafka_config()
        self.db_cfg = self.config_manager.get_db_config()
        self.candle_cfg = self.config_manager.get_candle_view_config()

        # Initialize Ingestor (DB Handler)
        self.ingestor = TimescaleIngestor(self.db_cfg, self.candle_cfg)

        # Kafka Settings
        self.topic = self.kafka_cfg.get('topic', 'crypto.futures.ticks')
        self.bootstrap_servers = self.kafka_cfg.get('bootstrap_servers', 'localhost:9092')
        self.batch_size = self.kafka_cfg.get('batch_size', 50)

    async def start(self):
        consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id="timescale_loader_group",
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        await consumer.start()

        logger.info(f"✅ Ingestor Started. Listening to: {self.topic}")

        try:
            batch = []
            async for msg in consumer:
                batch.append(msg.value)

                if len(batch) >= self.batch_size:
                    self._flush(batch)
                    batch = [] # Clear batch

        except Exception as e:
            logger.error(f"Ingestor loop error: {e}")
        finally:
            await consumer.stop()

    def _flush(self, batch):
        try:
            self.ingestor.save_batch(batch)
            logger.info(f"💾 Saved {len(batch)} ticks to DB.")
        except Exception as e:
            logger.error(f"Failed to save batch: {e}")


if __name__ == "__main__":
    service = DataIngestionService()
    try:
        asyncio.run(service.start())
    except KeyboardInterrupt:
        logger.info("Ingestor stopped by user.")
