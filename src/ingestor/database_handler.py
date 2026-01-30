import logging
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values


logger = logging.getLogger(__name__)


class TimescaleIngestor:
    def __init__(self, db_config):
        """
        Initialize TimescaleDB connection using config dictionary.
        """
        self.db_config = db_config
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
        create_table_query = """
        CREATE TABLE IF NOT EXISTS futures_ticks (
            time TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            price DOUBLE PRECISION NOT NULL
        );
        """
        # TimescaleDB hypertable command (time is the partitioning column)
        hypertable_query = "SELECT create_hypertable('futures_ticks', 'time', if_not_exists => TRUE);"

        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)
                cursor.execute(hypertable_query)
                conn.commit()
                logger.info("TimescaleDB Hypertable initialized.")

    def save_batch(self, data_batch):
        """
        Save data using bulk insert (execute_values) for high performance.
        """
        # Convert Binance ms timestamp to ISO format for PostgreSQL
        # Note: Binance 'E' is milliseconds
        if not data_batch:
            return

        query = "INSERT INTO futures_ticks (time, symbol, price) VALUES %s"

        values = []
        for data in data_batch:
            # Convert millisecond timestamp to Python datetime object
            # Binance event_time is in ms (e.g., 1769799311651)
            dt_object = datetime.fromtimestamp(data['event_time'] / 1000.0, tz=timezone.utc)
            values.append((dt_object, data['symbol'], data['price']))

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    execute_values(cursor, query, values)
                    conn.commit()
                    logger.info(f"Successfully ingested {len(data_batch)} records.")
        except Exception as e:
            logger.error(f"TimescaleDB Insert Error: {e}")
