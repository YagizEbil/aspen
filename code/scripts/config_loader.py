from __future__ import annotations

import json
from pathlib import Path


def load_named_json_config(config_path: str | None,
                           config_key: str | None,
                           repo_root: Path,
                           fail):
    if not config_path:
        return {}, None

    path = Path(config_path)
    if not path.is_absolute():
        path = repo_root / path
    if not path.exists():
        fail(f"Config file does not exist: {path}")

    try:
        data = json.loads(path.read_text())
    except Exception as exc:
        fail(f"Could not parse config file '{path}': {exc}")

    if config_key is not None:
        if not isinstance(data, dict):
            fail(f"Config file '{path}' must contain a JSON object when --config-key is used.")
        if config_key not in data:
            fail(f"Config key '{config_key}' was not found in: {path}")
        data = data[config_key]

    if not isinstance(data, dict):
        fail(f"Config payload from '{path}' must be a JSON object.")

    return data, path


def coerce_int(value, option_name: str, fail):
    if value is None:
        return None
    if isinstance(value, bool):
        fail(f"Config field '{option_name}' must be an integer, not a boolean.")
    try:
        return int(value)
    except (TypeError, ValueError):
        fail(f"Config field '{option_name}' must be an integer. Got: {value!r}")


def coerce_bool(value, option_name: str, fail):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    fail(f"Config field '{option_name}' must be a boolean. Got: {value!r}")


def coerce_int_list(value, option_name: str, fail):
    if value is None:
        return None
    if isinstance(value, str):
        parts = [part.strip() for part in value.split(",") if part.strip()]
        try:
            return [int(part) for part in parts]
        except ValueError as exc:
            fail(f"Could not parse config field '{option_name}': {exc}")
    if isinstance(value, list):
        try:
            return [int(part) for part in value]
        except (TypeError, ValueError) as exc:
            fail(f"Could not parse config field '{option_name}': {exc}")
    fail(f"Config field '{option_name}' must be a comma-separated string or integer list.")
