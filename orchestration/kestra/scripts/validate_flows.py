"""Perform repository-level structural checks on checked-in Kestra flows."""

from pathlib import Path

import yaml

FLOW_DIRECTORY = Path(__file__).resolve().parents[1] / "flows"


def main() -> None:
    identities: set[tuple[str, str]] = set()
    flow_paths = sorted(FLOW_DIRECTORY.glob("*.yaml"))
    if not flow_paths:
        raise SystemExit(f"No Kestra flows found in {FLOW_DIRECTORY}")

    for flow_path in flow_paths:
        flow = yaml.safe_load(flow_path.read_text(encoding="utf-8"))
        if not isinstance(flow, dict):
            raise TypeError(f"{flow_path} must contain one YAML mapping")

        required = {"id", "namespace", "tasks"}
        missing = required.difference(flow)
        if missing:
            raise ValueError(f"{flow_path} is missing: {', '.join(sorted(missing))}")
        if not isinstance(flow["tasks"], list) or not flow["tasks"]:
            raise ValueError(f"{flow_path} must define at least one task")

        identity = (str(flow["namespace"]), str(flow["id"]))
        if identity in identities:
            raise ValueError(f"Duplicate Kestra flow identity: {identity}")
        identities.add(identity)

    print(f"Validated {len(flow_paths)} Kestra flow files")


if __name__ == "__main__":
    main()
