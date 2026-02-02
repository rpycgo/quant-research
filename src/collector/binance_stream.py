import os
import json
import logging
import asyncio
import aiohttp
from websockets import connect


logger = logging.getLogger("BinanceCollector")


class BinanceCollector:
    def __init__(self, symbols):
        self.symbols = [s.lower() for s in symbols]
        self.base_url = "wss://fstream.binance.com/ws"
        self.rest_base_url = "https://fstream.binance.com/fapi/v1"
        self.handle_data = None # This will be the callback function
        self.running = False

        # Track the last seen trade ID for each symbol to detect gaps
        self.last_trade_ids = {symbol.upper(): None for symbol in self.symbols}
        self.gap_threshold = 1000

        self.headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        self.proxy_ip = os.getenv('SOURCE_IP')

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
                        # Use the dedicated handler to process messages and check for gaps
                        await self._handle_message(message)

            except Exception as e:
                logger.error(f"WebSocket Error: {e}. Retrying in 5s...")

                if not self.running:
                    break

                await asyncio.sleep(5) # Prevent tight loop on error

    async def _handle_message(self, message):
        data = json.loads(message)
        if data.get('e') == 'aggTrade':
            symbol = data['s']
            first_id = data['f']
            last_id = data['l']

            # Check for missing trade IDs before processing current data
            await self._check_and_fix_gap(symbol, first_id, last_id)

            parsed_tick = {
                'symbol': symbol,
                'price': float(data['p']),
                'volume': float(data['q']),
                'event_time': data['T'],
            }

            if self.handle_data:
                await self.handle_data(parsed_tick)

    async def _check_and_fix_gap(self, symbol, current_first_id, current_last_id):
        prev_last_id = self.last_trade_ids.get(symbol)

        if prev_last_id is not None:
            gap_size = current_first_id - prev_last_id - 1

            if 0 < gap_size <= self.gap_threshold:
                logger.warning(f"🚨 [Gap Detected] {symbol}: {gap_size} trades missing.")
                # Small gap detected: trigger immediate REST API call to fill the missing segment
                # We use asyncio.create_task to prevent blocking the main WebSocket loop
                asyncio.create_task(self._fetch_missing_trades(symbol, prev_last_id + 1, current_first_id - 1))

            elif gap_size > self.gap_threshold:
                # Massive gap detected: logging for external backfill worker or manual recovery
                # High-volume gaps should be handled by a dedicated service to avoid Rate Limit and Event Loop lag
                logger.error(f"⚠️ [Massive Gap] {symbol}: {gap_size} trades missing. Manual intervention or worker required.")

        self.last_trade_ids[symbol] = current_last_id

    async def _fetch_missing_trades(self, symbol, from_id, to_id):
        """
        Recover missing data using Binance REST API (Non-blocking).
        """
        try:
            url = f"{self.rest_base_url}/aggTrades?symbol={symbol}&fromId={from_id}&limit=1000"
            logger.info(f"🔍 [Attempting Fix] {symbol}: Requesting IDs {from_id} to {to_id}")

            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.get(url, proxy=self.proxy_ip, timeout=10) as response:
                    if response.status == 200:
                        missing_data = await response.json()
                        logger.info(f"📥 [API Response] {symbol}: Received {len(missing_data)} trades")
                        
                        count = 0
                        for trade in missing_data:
                            if trade['a'] > to_id: 
                                break
                            
                            parsed_fix = {
                                'symbol': symbol,
                                'price': float(trade['p']),
                                'volume': float(trade['q']),
                                'event_time': trade['T'],
                            }
                            await self.handle_data(parsed_fix)
                            count += 1

                        logger.info(f"✅ [Gap Fixed] {symbol}: Successfully recovered {count} trades.")
                    else:
                        logger.error(f"❌ [API Error] {symbol}: Status {response.status}, Reason: {response.reason}")
        except Exception as e:
            logger.error(f"❌ [Backfill Exception] {symbol}: {e}", exc_info=True)