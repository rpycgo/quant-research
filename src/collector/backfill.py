import time
import logging
import sys
import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from dotenv import load_dotenv

from src.utils.config_loader import ConfigManager
from src.ingestor.database_handler import TimescaleIngestor


# ---------------------------------------------------------
# PATH SETUP
# ---------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))

if project_root not in sys.path:
    sys.path.append(project_root)

load_dotenv(os.path.join(project_root, ".env"))

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger("Backfiller")

BASE_URL = "https://fapi.binance.com"
AGG_TRADES_ENDPOINT = "/fapi/v1/aggTrades"
LIMIT = 1000


# ---------------------------------------------------------
# CUSTOM ADAPTER FOR SOURCE IP BINDING
# ---------------------------------------------------------
class SourceAddressAdapter(HTTPAdapter):
    """
    Custom HTTPAdapter to bind a specific Source IP Address to the request.
    This allows forcing traffic through a specific network interface (e.g., VPN).
    """
    def __init__(self, source_address, **kwargs):
        self.source_address = source_address
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False):
        # Override the pool manager to bind the socket to the source_address
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            source_address=self.source_address
        )

class BackfillService:
    def __init__(self):
        """
        Initialize the BackfillService with Source IP configuration.
        """
        abs_config_path = os.path.join(project_root, "src", "config", "settings.yaml")

        self.config_manager = ConfigManager(config_path=abs_config_path)
        self.symbols = self.config_manager.get_symbols()
        self.db_cfg = self.config_manager.get_db_config()
        self.candle_view_cfg = self.config_manager.get_candle_view_config()

        self.ingestor = TimescaleIngestor(self.db_cfg, self.candle_view_cfg)

        # Initialize a Session for persistent connections
        self.session = requests.Session()

        # Configure Source IP if provided in .env
        source_ip = os.getenv("SOURCE_IP")
        if source_ip:
            logger.info(f"🔗 Binding outgoing requests to Source IP: {source_ip}")
            # Mount the custom adapter for both HTTP and HTTPS
            # The port 0 means "let the OS pick a random ephemeral port"
            adapter = SourceAddressAdapter(source_address=(source_ip, 0))
            self.session.mount("http://", adapter)
            self.session.mount("https://", adapter)
        else:
            logger.info("🌐 Using default network interface.")

    def find_gaps(self, symbol, lookback_hours=6, gap_threshold_sec=60):
        """
        Detects time gaps in the database.
        """
        logger.info(f"🔎 Detecting gaps for {symbol} (Last {lookback_hours} hours)...")
        conn = self.ingestor._get_connection()
        gaps = []
        try:
            with conn.cursor() as cursor:
                query = """
                SELECT time, next_time, gap_seconds
                FROM (
                    SELECT 
                        time, 
                        LEAD(time) OVER (ORDER BY time) AS next_time,
                        EXTRACT(EPOCH FROM (LEAD(time) OVER (ORDER BY time) - time)) AS gap_seconds
                    FROM futures_ticks
                    WHERE symbol = %s 
                      AND time > NOW() - INTERVAL '%s hours'
                ) AS sub
                WHERE gap_seconds > %s
                ORDER BY time DESC;
                """
                cursor.execute(query, (symbol, lookback_hours, gap_threshold_sec))
                gaps = cursor.fetchall()
        except Exception as e:
            logger.error(f"Error checking gaps: {e}")
        finally:
            conn.close()
        return gaps

    def _fetch_agg_trades(self, symbol, start_ts, end_ts):
        """
        Fetches data using the configured session (with Source IP binding).
        """
        params = {
            "symbol": symbol, 
            "startTime": start_ts, 
            "endTime": end_ts, 
            "limit": LIMIT
        }
        try:
            # Use self.session instead of requests.get
            response = self.session.get(
                f"{BASE_URL}{AGG_TRADES_ENDPOINT}", 
                params=params, 
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"API Request Failed: {e}")
            return []

    def run(self):
        """
        Main execution loop.
        """
        if not self.symbols:
            logger.error("No target symbols found in settings.yaml")
            return

        for symbol in self.symbols:
            logger.info(f"=== Processing {symbol} ===")

            gaps = self.find_gaps(symbol, lookback_hours=6, gap_threshold_sec=60)

            if gaps:
                logger.info(f"⚠️ Found {len(gaps)} gaps! Starting repair...")
                for start_time, end_time, gap_sec in gaps:
                    logger.info(f"🔧 Repairing gap: {gap_sec:.1f}s ({start_time} ~ {end_time})")

                    start_ts_ms = int(start_time.timestamp() * 1000) + 1
                    end_ts_ms = int(end_time.timestamp() * 1000) - 1

                    self._process_range(symbol, start_ts_ms, end_ts_ms)
            else:
                logger.info("✅ No gaps found in the middle.")

    def _process_range(self, symbol, start_ts, end_ts):
        current_start = start_ts
        total_ingested = 0

        while current_start < end_ts:
            trades = self._fetch_agg_trades(symbol, current_start, end_ts)
            if not trades:
                break

            batch_data = []
            last_trade_time = current_start

            for t in trades:
                batch_data.append({
                    "symbol": symbol,
                    "price": float(t['p']),
                    "event_time": t['T']
                })
                last_trade_time = t['T']

            if batch_data:
                self.ingestor.save_batch(batch_data)
                total_ingested += len(batch_data)

            current_start = last_trade_time + 1
            time.sleep(0.1)

        logger.info(f"   -> Filled {total_ingested} records for this gap.")


if __name__ == "__main__":
    try:
        service = BackfillService()
        service.run()
    except KeyboardInterrupt:
        logger.info("Stopped by user.")
    except Exception as e:
        logger.critical(f"Unexpected error: {e}")
