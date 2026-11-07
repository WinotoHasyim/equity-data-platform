"""Generate Kestra's ignored, base64-encoded local secrets file."""

from __future__ import annotations

import base64
import os
from pathlib import Path

from env_utils import read_env

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SOURCE_PATH = PROJECT_ROOT / ".env"
TARGET_PATH = PROJECT_ROOT / ".env.kestra"
REQUIRED_KEYS = ("APCA_API_KEY_ID", "APCA_API_SECRET_KEY")
def main() -> None:
    if not SOURCE_PATH.is_file():
        raise SystemExit(f"Missing {SOURCE_PATH}; create it from .env.example first")

    values = read_env(SOURCE_PATH)
    missing = [key for key in REQUIRED_KEYS if not values.get(key)]
    if missing:
        raise SystemExit(f"Missing required values in .env: {', '.join(missing)}")

    lines = []
    for key in REQUIRED_KEYS:
        encoded = base64.b64encode(values[key].encode()).decode("ascii")
        lines.append(f"SECRET_{key}={encoded}")

    temporary_path = TARGET_PATH.with_suffix(".tmp")
    temporary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.chmod(temporary_path, 0o600)
    temporary_path.replace(TARGET_PATH)
    print(f"Generated {TARGET_PATH} with permissions 0600")


if __name__ == "__main__":
    main()
