import os
import pathlib
import yaml


class ConfigManager:
    def __init__(self, config_path: str | None = None):
        if config_path is None:
            project_root = pathlib.Path(__file__).resolve().parent.parent
            config_path = project_root / "configs" / "pipeline_settings.yaml"
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

    def get_kafka_config(self):
        return self._config.get('kafka', {})
