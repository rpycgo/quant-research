import logging
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values


logger = logging.getLogger(__name__)


class TimescaleIngestor:
    def __init__(self, db_config, candle_config=None):
        """
        Initialize TimescaleDB connection using config dictionary.
        """
        self.db_config = db_config
        self.candle_config = candle_config or []
        self._init_db()

    def _get_connection(self):
        """Create a new PostgreSQL connection."""
        return psycopg2.connect(
            host=self.db_config['host'],
            port=self.db_config['port'],
            database=self.db_config['dbname'],
            user=self.db_config['user'],
            password=self.db_config['password']
        )

    def _init_db(self):
        """
        Create table and convert it to a TimescaleDB hypertable.
        """
        conn = self._get_connection()
        conn.autocommit = True

        try:
            with conn.cursor() as cursor:
                # 1. Initialize base table and convert to hypertable
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS future_ticks (
                        time TIMESTAMPTZ NOT NULL,
                        symbol TEXT NOT NULL,
                        price DOUBLE PRECISION NOT NULL,
                        volume DOUBLE PRECISION NOT NULL
                    );
                """)
                cursor.execute("SELECT create_hypertable('future_ticks', 'time', if_not_exists => TRUE);")

                # 2. Create continuous aggregates dynamically from config
                for cfg in self.candle_config:
                    query = self._build_candle_query(cfg)
                    cursor.execute(query)

                logger.info(f"Initialized {len(self.candle_config)} candle views from config.")

        except Exception as e:
            logger.error(f"DB Initialization Error: {e}")
            raise

        finally:
            conn.close()

    def _build_candle_query(self, cfg):
        """
        Build SQL query dynamically. 
        Handles the column name difference: 'time' for raw ticks vs 'bucket' for existing views.
        """
        if cfg['source'] == "future_ticks":
            # Direct aggregation from raw tick data
            # Use 'time' as the ordering column
            return f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {cfg['name']} 
            WITH (timescaledb.continuous = true) AS
            SELECT time_bucket('{cfg['interval']}', time) AS bucket, symbol,
                   first(price, time) as open, 
                   max(price) as high, 
                   min(price) as low, 
                   last(price, time) as close,
                   SUM(volume) as volume
            FROM future_ticks 
            GROUP BY time_bucket('{cfg['interval']}', time), symbol;
            """
        else:
            # Hierarchical aggregation from an existing materialized view
            # Use 'bucket' as the ordering column since it's the time column in the source view
            return f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {cfg['name']} 
            WITH (timescaledb.continuous = true) AS
            SELECT time_bucket('{cfg['interval']}', bucket) AS bucket, symbol,
                   first(open, bucket) as open, 
                   max(high) as high, 
                   min(low) as low, 
                   last(close, bucket) as close,
                   SUM(volume) as volume
            FROM {cfg['source']} 
            GROUP BY time_bucket('{cfg['interval']}', bucket), symbol;
            """

    def save_batch(self, data_batch):
        """
        Save data using bulk insert (execute_values) for high performance.
        """
        # Convert Binance ms timestamp to ISO format for PostgreSQL
        # Note: Binance 'E' is milliseconds
        if not data_batch:
            return

        query = "INSERT INTO future_ticks (time, symbol, price, volume) VALUES %s"

        values = []
        for data in data_batch:
            # Convert millisecond timestamp to Python datetime object
            # Binance event_time is in ms (e.g., 1769799311651)
            dt_object = datetime.fromtimestamp(data['event_time'] / 1000.0, tz=timezone.utc)
            values.append((dt_object, data['symbol'], data['price'], data['volume']))

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    execute_values(cursor, query, values)
                    conn.commit()
                    logger.info(f"Successfully ingested {len(data_batch)} records.")
        except Exception as e:
            logger.error(f"TimescaleDB Insert Error: {e}")
