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
        stream_names = [f"{symbol}@aggTrade" for symbol in self.symbols]
        stream_url = f"{self.base_url}/{'/'.join(stream_names)}"

        self.running = True
        logger.info(f"Connected to streams: {stream_names}")

        while self.running:
            try:
                async with connect(
                    stream_url,
                    ping_interval=20,
                    ping_timeout=10,
                    ) as websocket:
                    logger.info("✅ WebSocket Connection established.")

                    while self.running:
                        message = await websocket.recv()
                        data = json.loads(message)

                        # Parse data needed for simple tick
                        # e: event type, s: symbol, p: price, T: event time, q: volume
                        if 'e' in data and data['e'] == 'aggTrade':
                            parsed_tick = {
                                'symbol': data['s'],
                                'price': float(data['p']),
                                'volume': float(data['q']),
                                'event_time': data['T'],
                            }

                            if self.handle_data:
                                # Callback function is async, so we MUST await it!
                                await self.handle_data(parsed_tick)

            except Exception as e:
                logger.error(f"WebSocket Error: {e}. Retrying in 5s...")

                if not self.running:
                    break

                await asyncio.sleep(5) # Prevent tight loop on error
