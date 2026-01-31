import asyncio
import json
import logging
from websockets import connect


logger = logging.getLogger("BinanceCollector")


class BinanceCollector:
    def __init__(self, symbols):
        self.symbols = [s.lower() for s in symbols]
        self.base_url = "wss://fstream.binance.com/ws"
        self.handle_data = None # This will be the callback function
        self.running = False

    async def start(self):
        """
        Connect to Binance WebSocket and stream data.
        """
        # Combine streams: btcusdt@ticker/ethusdt@ticker
        stream_names = [f"{s}@ticker" for s in self.symbols]
        stream_url = f"{self.base_url}/{'/'.join(stream_names)}"

        self.running = True
        logger.info(f"Connected to streams: {stream_names}")

        async with connect(stream_url) as websocket:
            while self.running:
                try:
                    message = await websocket.recv()
                    data = json.loads(message)

                    # Parse data needed for simple tick
                    # e: event type, s: symbol, p: price, E: event time
                    if 'e' in data and data['e'] == '24hrTicker':
                        parsed_tick = {
                            'symbol': data['s'],
                            'price': float(data['c']), # 'c' is last price in ticker stream
                            'event_time': data['E']
                        }

                        if self.handle_data:
                            # Callback function is async, so we MUST await it!
                            await self.handle_data(parsed_tick)

                except Exception as e:
                    logger.error(f"WebSocket Error: {e}")
                    await asyncio.sleep(1) # Prevent tight loop on error
