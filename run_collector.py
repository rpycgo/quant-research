import asyncio
import logging
import sys
from dotenv import load_dotenv

from src.utils.config_loader import ConfigManager
from src.collector.binance_stream import BinanceCollector
from src.collector.kafka_producer import AsyncKafkaProducer


load_dotenv()

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [COLLECTOR] - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger("CollectorService")


class CryptoCollectorService:
    def __init__(self):
        self.config_manager = ConfigManager()
        self.symbols = self.config_manager.get_symbols()
        self.kafka_cfg = self.config_manager.get_kafka_config()

        # Components
        self.collector = BinanceCollector(self.symbols)
        self.producer = AsyncKafkaProducer(
            bootstrap_servers=self.kafka_cfg.get('bootstrap_servers', 'localhost:9092'),
            topic=self.kafka_cfg.get('topic', 'crypto.futures.ticks')
        )

    async def _data_handler(self, data):
        # WebSocket -> Kafka
        await self.producer.send(data)

    async def start(self):
        if not self.symbols:
            logger.error("No symbols found.")
            return

        await self.producer.start()
        self.collector.handle_data = self._data_handler

        logger.info(f"🚀 Collector Started. Target: {self.symbols}")
        try:
            await self.collector.start()
        except Exception as e:
            logger.critical(f"Collector crashed: {e}")
        finally:
            await self.producer.stop()


if __name__ == "__main__":
    service = CryptoCollectorService()
    try:
        asyncio.run(service.start())
    except KeyboardInterrupt:
        logger.info("Collector stopped by user.")
