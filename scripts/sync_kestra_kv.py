"""Synchronize Terraform and local image configuration into Kestra KV storage."""

from __future__ import annotations

import base64
import json
import os
import subprocess
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from env_utils import read_env

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"
NAMESPACE = "equity.dev"


def terraform_outputs() -> dict[str, object]:
    result = subprocess.run(
        ["terraform", "-chdir=terraform", "output", "-json"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    raw_outputs = json.loads(result.stdout)
    return {key: output["value"] for key, output in raw_outputs.items()}


def put_kv(base_url: str, username: str, password: str, key: str, value: str) -> None:
    credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
    request = Request(
        f"{base_url}/api/v1/main/namespaces/{NAMESPACE}/kv/{key}",
        data=json.dumps(value).encode(),
        method="PUT",
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "text/plain",
        },
    )
    with urlopen(request, timeout=10) as response:
        if response.status not in {200, 201, 204}:
            raise RuntimeError(f"Unexpected Kestra response for {key}: {response.status}")


def main() -> None:
    if not ENV_PATH.is_file():
        raise SystemExit(f"Missing {ENV_PATH}; create it from .env.example first")

    env = read_env(ENV_PATH)
    env.update(
        {
            key: os.environ[key]
            for key in (
                "KESTRA_ADMIN_EMAIL",
                "KESTRA_ADMIN_PASSWORD",
                "KESTRA_URL",
                "PIPELINE_IMAGE",
            )
            if key in os.environ
        }
    )
    required = ("KESTRA_ADMIN_EMAIL", "KESTRA_ADMIN_PASSWORD")
    missing = [key for key in required if not env.get(key)]
    if missing:
        raise SystemExit(f"Missing required values in .env: {', '.join(missing)}")

    password = env["KESTRA_ADMIN_PASSWORD"]
    password_requirements = {
        "at least 8 characters": len(password) >= 8,
        "an uppercase letter": any(character.isupper() for character in password),
        "a lowercase letter": any(character.islower() for character in password),
        "a digit": any(character.isdigit() for character in password),
    }
    unmet = [label for label, satisfied in password_requirements.items() if not satisfied]
    if unmet:
        raise SystemExit(
            "KESTRA_ADMIN_PASSWORD does not satisfy Kestra's password policy; "
            f"it needs {', '.join(unmet)}"
        )

    outputs = terraform_outputs()
    datasets = outputs["bigquery_dataset_ids"]
    values = {
        "BQ_ANALYTICS_DATASET": datasets["analytics"],
        "BQ_RAW_DATASET": datasets["raw"],
        "GCP_ADC_HOST_PATH": str(
            Path.home()
            / ".config"
            / "gcloud"
            / "application_default_credentials.json"
        ),
        "GCP_LOCATION": outputs["location"],
        "GCP_PROJECT_ID": outputs["project_id"],
        "GCS_RAW_BUCKET": outputs["raw_bucket_name"],
        "PIPELINE_IMAGE": env.get("PIPELINE_IMAGE", "equity-data-platform:dev"),
    }

    base_url = env.get("KESTRA_URL", "http://localhost:8080").rstrip("/")
    try:
        for key, value in values.items():
            put_kv(
                base_url,
                env["KESTRA_ADMIN_EMAIL"],
                env["KESTRA_ADMIN_PASSWORD"],
                key,
                str(value),
            )
    except (HTTPError, URLError) as exc:
        raise SystemExit(f"Failed to synchronize Kestra KV values: {exc}") from exc

    print(f"Synchronized {len(values)} KV values in namespace {NAMESPACE}")


if __name__ == "__main__":
    main()
