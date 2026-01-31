import os
import logging
from binance import AsyncClient, BinanceSocketManager
from dotenv import load_dotenv


load_dotenv()
logger = logging.getLogger(__name__)


class BinanceCollector:
    def __init__(self, symbols):
        # Convert to Binance WebSocket format (e.g., btcusdt@ticker)
        self.stream_names = [f"{symbol.lower()}@ticker" for symbol in symbols]
        self.client = None
        self.handle_data = None  # Callback assigned by main pipeline

    async def start(self):
        """Start the real-time WebSocket stream."""
        try:
            self.client = await AsyncClient.create(
                api_key=os.getenv('BINANCE_API_KEY'),
                api_secret=os.getenv('BINANCE_API_SECRET'),
                tld='us',
            )
            bm = BinanceSocketManager(self.client)
            ms = bm.multiplex_socket(self.stream_names)

            async with ms as stream:
                logger.info(f"Connected to streams: {self.stream_names}")
                while True:
                    res = await stream.recv()
                    if 'data' in res:
                        data = res['data']
                        parsed_tick = {
                            "symbol": data['s'],
                            "price": float(data['c']),
                            "event_time": data['E']
                        }
                        if self.handle_data:
                            self.handle_data(parsed_tick)
        except Exception as e:
            logger.error(f"WebSocket Connection Error: {e}")
        finally:
            if self.client:
                await self.client.close_connection()
