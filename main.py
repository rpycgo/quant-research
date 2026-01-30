import asyncio
import logging
import sys
from src.utils.config_loader import ConfigManager
from src.collector.binance_stream import BinanceCollector
from src.ingestor.database_handler import TimescaleIngestor


# Basic logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)


class CryptoPipeline:
    def __init__(self):
        """
        Initialize the pipeline by loading configs and setting up components.
        """
        # 1. Load Configurations
        self.config_manager = ConfigManager()
        self.symbols = self.config_manager.get_symbols()

        pipeline_cfg = self.config_manager.get_pipeline_config()
        db_cfg = self.config_manager.get_db_config()

        # 2. Initialize Ingestor (TimescaleDB) and Worker (WebSocket)
        self.ingestor = TimescaleIngestor(db_cfg)
        self.collector = BinanceCollector(self.symbols)

        self.batch_size = pipeline_cfg.get('batch_size', 20)
        self.buffer = []

    def _data_handler(self, data):
        """
        Callback function to handle incoming ticker data from the collector.
        """
        self.buffer.append(data)

        # Log every tick for debugging (optional)
        # logger.debug(f"Received: {data['symbol']} @ {data['price']}")
        if len(self.buffer) >= self.batch_size:
            logger.info(f"Batch size reached ({len(self.buffer)}). Ingesting to TimescaleDB...")
            self.ingestor.save_batch(self.buffer)
            self.buffer.clear()

    async def start(self):
        """
        Setup the handler and start the asynchronous streaming.
        """
        if not self.symbols:
            logger.error("No target symbols found in config. Please check settings.yaml.")
            return

        # Inject the handler logic into the collector
        self.collector.handle_data = self._data_handler

        logger.info(f"Starting pipeline for symbols: {self.symbols}")
        try:
            await self.collector.start()
        except Exception as e:
            logger.error(f"Pipeline crashed: {e}")

if __name__ == "__main__":
    # Initialize the main pipeline
    pipeline = CryptoPipeline()

    # Run the event loop
    try:
        asyncio.run(pipeline.start())
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user (Ctrl+C).")
    except Exception as e:
        logger.critical(f"Unexpected error: {e}")
