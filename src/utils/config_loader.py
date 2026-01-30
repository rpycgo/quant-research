import os
import yaml


class ConfigManager:
    def __init__(self, config_path="src/config/settings.yaml"):
        self.config_path = config_path
        self._config = self._load_config()

    def _load_config(self):
        """Load YAML configuration file."""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        with open(self.config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def get_symbols(self):
        return self._config.get("target_symbols", [])

    def get_pipeline_config(self):
        return self._config.get("pipeline", {})

    def get_db_config(self):
        return self._config.get('database', {})

    def get_candle_view_config(self):
        return self._config.get('candle_views', {})
