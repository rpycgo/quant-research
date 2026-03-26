"""
backtesting.core.config_loader
==============================
Three-layer hierarchical configuration loader.

Layer resolution order (later layers override earlier ones):

1. **Package default** — ``default_config.toml`` shipped inside the external
   model package (e.g. ``mdrs_sde/configs/default_config.toml``).  This
   layer is the model author's baseline and is loaded via
   :mod:`importlib.resources` so it works regardless of install location.

2. **Local override** — ``configs/model_parameters/<model_key>.toml`` inside
   the ``quant-research`` repository.  Only the keys that differ from the
   default need to be listed here, keeping diffs minimal and experiment-
   specific changes immediately visible in ``git diff``.

3. **Shared infrastructure** — ``configs/backtest_settings.toml`` and
   ``configs/data_settings.toml``.  These are asset-class and model agnostic
   (execution costs, WFA schedule, data-collection parameters).

Usage::

    loader = BacktestConfigLoader()

    # model config: package default merged with local override
    cfg = loader.get_model_config("mdrs_sde_btc")

    # shared infra configs (unchanged from existing quant-research repo)
    bt  = loader.get_backtest_settings()
    ds  = loader.get_data_settings()
"""
from __future__ import annotations

import pathlib
import tomllib
from typing import Any

# ---------------------------------------------------------------------------
# Mapping: model_key → importlib.resources package path
# Register new model packages here when they are added to pyproject.toml.
# ---------------------------------------------------------------------------
_MODEL_PACKAGE_MAP: dict[str, str] = {
    "mdrs_sde_btc": "mdrs_sde.configs",
    "mdrs_sde_eth": "mdrs_sde.configs",
    "garch_btc": "garch_model.configs",
    "garch_eth": "garch_model.configs",
}

_DEFAULT_CONFIG_FILENAME = "default_config.toml"


class BacktestConfigLoader:
    """Hierarchical TOML configuration loader for the backtesting module.

    Attributes:
        project_root: Absolute path to the repository root (two levels above
            this file: ``backtesting/core/config_loader.py``).
        config_dir:   Absolute path to ``configs/`` inside the repository.
    """

    def __init__(self, config_dir: str = "configs") -> None:
        """Initialise the loader and resolve all directory paths.

        Args:
            config_dir: Path to the configuration directory relative to the
                project root.  Defaults to ``"configs"``.
        """
        # backtesting/core/config_loader.py → project root (3 parents up)
        self.project_root: pathlib.Path = (
            pathlib.Path(__file__).resolve().parent.parent.parent
        )
        self.config_dir: pathlib.Path = self.project_root / config_dir

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_model_config(self, model_key: str) -> dict[str, Any]:
        """Return the merged configuration for *model_key*.

        Loads the package default first, then deep-merges any local override
        on top, so only the keys listed in the override file change.

        Args:
            model_key: Identifier matching an entry in ``_MODEL_PACKAGE_MAP``
                and a ``.toml`` filename inside ``configs/model_parameters/``.
                Examples: ``"mdrs_sde_btc"``, ``"garch_eth"``.

        Returns:
            Merged configuration dictionary.

        Raises:
            KeyError: If *model_key* is not registered in ``_MODEL_PACKAGE_MAP``.
            FileNotFoundError: If the external package's default config is
                missing (the package may not be installed).
        """
        if model_key not in _MODEL_PACKAGE_MAP:
            raise KeyError(
                f"Unknown model key '{model_key}'. "
                f"Register it in _MODEL_PACKAGE_MAP inside config_loader.py. "
                f"Available keys: {list(_MODEL_PACKAGE_MAP)}"
            )

        # Layer 1: external package default
        base_config = self._load_package_default(model_key)

        # Layer 2: local override (optional — missing file is silently skipped)
        override_path = (
            self.config_dir / "model_parameters" / f"{model_key}.toml"
        )
        if override_path.exists():
            override = self._read_toml(override_path)
            base_config = self._deep_merge(base_config, override)

        return base_config

    def get_backtest_settings(self) -> dict[str, Any]:
        """Return shared backtest-engine configuration.

        Reads ``configs/backtest_settings.toml`` — execution costs, trading
        parameters, risk management, WFA schedule, and filter toggles.

        Returns:
            Parsed configuration dictionary.
        """
        return self._read_toml(self.config_dir / "backtest_settings.toml")

    def get_data_settings(self) -> dict[str, Any]:
        """Return shared data-pipeline configuration.

        Reads ``configs/data_settings.toml`` — collection parameters,
        preprocessing windows, and event-detection thresholds.

        Returns:
            Parsed configuration dictionary.
        """
        return self._read_toml(self.config_dir / "data_settings.toml")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _load_package_default(self, model_key: str) -> dict[str, Any]:
        """Load ``default_config.toml`` from the external model package.

        Uses :mod:`importlib.resources` so the file is resolved from the
        installed package tree, not from a hard-coded filesystem path.

        Args:
            model_key: Registered model identifier.

        Returns:
            Parsed default configuration dictionary.

        Raises:
            FileNotFoundError: If the package or its config file is absent.
        """
        from importlib.resources import files  # Python 3.9+

        package_path = _MODEL_PACKAGE_MAP[model_key]
        try:
            resource = files(package_path).joinpath(_DEFAULT_CONFIG_FILENAME)
            with resource.open("rb") as fh:
                return tomllib.load(fh)
        except (ModuleNotFoundError, FileNotFoundError) as exc:
            raise FileNotFoundError(
                f"Could not load default config for '{model_key}'. "
                f"Ensure the package exporting '{package_path}' is installed "
                f"(e.g. `pip install git+https://github.com/rpycgo/<repo>.git`). "
                f"Original error: {exc}"
            ) from exc

    @staticmethod
    def _read_toml(path: pathlib.Path) -> dict[str, Any]:
        """Parse a TOML file and return its contents as a dict.

        Args:
            path: Absolute path to the ``.toml`` file.

        Returns:
            Parsed dictionary.

        Raises:
            FileNotFoundError: If *path* does not exist.
        """
        if not path.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {path}"
            )
        with open(path, "rb") as fh:
            return tomllib.load(fh)

    @staticmethod
    def _deep_merge(
        base: dict[str, Any],
        override: dict[str, Any],
    ) -> dict[str, Any]:
        """Recursively merge *override* into *base*.

        Only keys present in *override* are changed; all other keys from
        *base* are preserved.  Nested dicts are merged recursively;
        non-dict values are replaced outright.

        Args:
            base:     The starting dictionary (package default).
            override: Keys to overwrite (local experiment config).

        Returns:
            A new merged dictionary (neither input is mutated).
        """
        result = base.copy()
        for key, value in override.items():
            if isinstance(value, dict) and isinstance(result.get(key), dict):
                result[key] = BacktestConfigLoader._deep_merge(
                    result[key], value
                )
            else:
                result[key] = value
        return result
