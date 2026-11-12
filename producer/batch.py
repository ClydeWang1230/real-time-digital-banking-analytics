"""Immutable JSONL batches: prepare before publishing and replay saved bytes."""

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path

from config import PROJECT_ROOT
from consumer.validation import validate_event
from producer.event_generator import GENERATOR_VERSION, SimulationSpec, generate_events


BATCH_FORMAT = "banking-batch-v1"
DEFAULT_BATCH_DIR = PROJECT_ROOT / "artifacts" / "runs"


@dataclass(frozen=True)
class Batch:
    directory: Path
    manifest: dict
    messages: list[tuple[bytes, bytes]]


def project_path(path: str | Path) -> Path:
    path = Path(path).expanduser()
    return (path if path.is_absolute() else PROJECT_ROOT / path).resolve()


def encode_event(event: dict) -> bytes:
    return json.dumps(event, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _new_file(path: Path, data: bytes) -> None:
    with path.open("xb") as target:  # Never overwrite an existing artifact.
        target.write(data)
        target.flush()
        os.fsync(target.fileno())


def prepare_batch(spec: SimulationSpec, output_dir: str | Path = DEFAULT_BATCH_DIR) -> Batch:
    events = list(generate_events(spec))
    data = b"".join(encode_event(event) + b"\n" for event in events)
    manifest = {"format": BATCH_FORMAT, "simulation": spec.as_dict(),
                "event_count": len(events), "events_sha256": hashlib.sha256(data).hexdigest()}
    directory = project_path(output_dir) / spec.run_id
    if directory.exists():
        batch = load_batch(directory)
        if batch.manifest != manifest:
            raise ValueError("This run_id already has different inputs or events; use a new run_id for a new simulation")
        return batch

    directory.mkdir(parents=True, exist_ok=False)
    _new_file(directory / "events.jsonl", data)
    # The manifest is the completion marker. An interrupted preparation is never published.
    _new_file(directory / "manifest.json", (json.dumps(manifest, indent=2) + "\n").encode("utf-8"))
    return load_batch(directory)


def load_batch(directory: str | Path) -> Batch:
    """Read and validate the ENTIRE batch before any Kafka producer is created."""
    directory = project_path(directory)
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or manifest.get("format") != BATCH_FORMAT:
        raise ValueError("Unsupported or incomplete batch manifest")
    simulation = manifest.get("simulation")
    required = {"generator_version", "run_id", "seed", "base_time", "customer_count"}
    if not isinstance(simulation, dict) or set(simulation) != required:
        raise ValueError("Manifest must contain all simulation inputs")
    if simulation["generator_version"] != GENERATOR_VERSION:
        raise ValueError("Unsupported generator version in manifest")
    if not isinstance(simulation["run_id"], str) or not isinstance(simulation["base_time"], str):
        raise ValueError("Manifest run_id and base_time must be strings")
    spec = SimulationSpec.create(simulation["customer_count"], run_id=simulation["run_id"],
                                 seed=simulation["seed"], base_time=simulation["base_time"])
    data = (directory / "events.jsonl").read_bytes()
    if hashlib.sha256(data).hexdigest() != manifest.get("events_sha256"):
        raise ValueError("Batch checksum mismatch; no messages were published")
    if not data or not data.endswith(b"\n"):
        raise ValueError("Batch must be nonempty JSONL with a final newline")
    messages = []
    event_ids, customers, sessions = set(), set(), set()
    for payload in data.split(b"\n")[:-1]:
        event = validate_event(payload).data
        if (event.get("run_id"), event.get("generator_version"), event.get("simulation_seed"),
            event.get("simulation_base_time")) != (
                spec.run_id, GENERATOR_VERSION, spec.seed, spec.base_time.isoformat()):
            raise ValueError("Event batch metadata does not match the manifest")
        if event["event_id"] in event_ids:
            raise ValueError("A prepared batch must contain unique event IDs")
        event_ids.add(event["event_id"])
        customers.add(event["customer_id"])
        sessions.add(event["session_id"])
        messages.append((event["customer_id"].encode("utf-8"), payload))
    if type(manifest.get("event_count")) is not int or manifest["event_count"] != len(messages):
        raise ValueError("Batch event count does not match the manifest")
    if len(customers) != spec.customer_count or len(sessions) != spec.customer_count:
        raise ValueError("Batch customer/session counts do not match the manifest")
    return Batch(directory, manifest, messages)
