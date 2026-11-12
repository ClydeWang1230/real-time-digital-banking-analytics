"""Validate one banking event; journey-level checks belong to the SQL layer."""

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import re
from typing import Any
from uuid import UUID


EVENT_TYPES = (
    "onboarding_started", "profile_submitted", "kyc_submitted",
    "kyc_verification_completed", "account_opened", "card_activated",
    "first_transaction_completed",
)
CHANNEL_DEVICES = {
    "mobile_app": {"ios", "android"},
    "web": {"desktop"},
    "branch": {"branch_terminal"},
}
REQUIRED_FIELDS = {
    "event_id", "customer_id", "event_type", "event_time",
    "channel", "device_type", "session_id", "properties",
}


class EventValidationError(ValueError):#ValueError是父类，是Pyhon 官方（标准库）在底层定义好的一个“基础报错模板”
    def __init__(self, code: str, reason: str): #子类EVE调动父类VE, REASON是参数
        super().__init__(reason)
        self.code = code


@dataclass(frozen=True)
class ValidatedEvent:
    data: dict[str, Any]
    canonical_json: str
    payload_hash: str


def _text(data: dict, name: str) -> str:
    value = data.get(name)
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise EventValidationError("invalid_field", f"{name} must be a non-blank string without surrounding whitespace")
    return value


def _uuid(data: dict, name: str) -> None:
    value = _text(data, name)
    try:
        data[name] = str(UUID(value))
    except ValueError as exc:
        raise EventValidationError("invalid_uuid", f"{name} must be a UUID") from exc


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise EventValidationError("duplicate_json_key", "JSON objects must not contain repeated keys")
        result[key] = value
    return result


def _invalid_constant(value: str) -> None:
    raise EventValidationError("invalid_json_number", "JSON must not contain NaN or Infinity")
#JSON不允许含有NaN

def validate_event(payload: bytes | None, key: bytes | None = None) -> ValidatedEvent:
    """Decode, check and normalize an event. Preserve extra fields in its payload."""
    if payload is None:
        raise EventValidationError("null_payload", "Tombstones are not banking events")
    try:
        event = json.loads(
            payload.decode("utf-8"), object_pairs_hook=_unique_keys,
            parse_constant=_invalid_constant,
        )
    except UnicodeDecodeError as exc: #如果出现不合法UTF-8编码，把python自带的UDError转成自设计的EVE,异常对象赋给变量exc
        raise EventValidationError("invalid_utf8", "Payload must be UTF-8") from exc
    except EventValidationError:
        raise #如果捕获到的本来就是 EventValidationError，就原样重新抛出，不要修改它。比如前面定义的出现NaN
    except (ValueError, RecursionError) as exc: #Python原本对JSON判断的ValueError和JSON嵌套RecursionError也被赋给我们自定义的EVE
        raise EventValidationError("invalid_json", "Payload must be valid JSON within parser depth limits") from exc
    if not isinstance(event, dict):
        raise EventValidationError("invalid_object", "The event must be a JSON object")
    missing = REQUIRED_FIELDS - event.keys()
    if missing:
        raise EventValidationError("missing_fields", "Missing required fields: " + ", ".join(sorted(missing)))

    _uuid(event, "event_id")
    for name in ("customer_id", "session_id", "event_type", "channel", "device_type"):
        _text(event, name)
    if event["event_type"] not in EVENT_TYPES:
        raise EventValidationError("unknown_event_type", "event_type is not a supported onboarding stage")
    if event["channel"] not in CHANNEL_DEVICES:
        raise EventValidationError("invalid_channel", "channel is not supported")
    if event["device_type"] not in CHANNEL_DEVICES[event["channel"]]:
        raise EventValidationError("invalid_device", "device_type does not match channel")

    try:
        event_time = datetime.fromisoformat(_text(event, "event_time"))
        if event_time.utcoffset() is None:
            raise ValueError("timezone missing")
        event["event_time"] = event_time.astimezone(timezone.utc).isoformat()
    except (ValueError, OverflowError) as exc:
        raise EventValidationError("invalid_event_time", "event_time must be an ISO timestamp with a timezone") from exc

    # Legacy P0.2 events remain valid. New simulation metadata is all-or-nothing.
    batch_fields = {"run_id", "generator_version", "simulation_seed", "simulation_base_time"}
    if batch_fields & event.keys():
        if not batch_fields.issubset(event):
            raise EventValidationError("incomplete_batch_metadata", "All four simulation metadata fields are required together")
        _uuid(event, "run_id")
        _text(event, "generator_version")
        if type(event["simulation_seed"]) is not int or not 0 <= event["simulation_seed"] <= 2**63 - 1:
            raise EventValidationError("invalid_simulation_seed", "simulation_seed must be an integer between 0 and 2**63 - 1")
        try:
            base_time = datetime.fromisoformat(_text(event, "simulation_base_time"))
            if base_time.utcoffset() is None:
                raise ValueError("timezone missing")
            event["simulation_base_time"] = base_time.astimezone(timezone.utc).isoformat()
        except (ValueError, OverflowError) as exc:
            raise EventValidationError("invalid_simulation_base_time", "simulation_base_time must include a timezone") from exc

    if key is not None:
        try:
            if key.decode("utf-8") != event["customer_id"]:
                raise EventValidationError("key_mismatch", "Kafka key must match customer_id when present")
        except UnicodeDecodeError as exc:
            raise EventValidationError("invalid_key", "Kafka key must be UTF-8 when present") from exc

    properties = event["properties"]
    if not isinstance(properties, dict):
        raise EventValidationError("invalid_properties", "properties must be a JSON object")
    required_text = {
        "onboarding_started": ("campaign_id", "entry_point"),
        "profile_submitted": ("country", "occupation_group"),
        "kyc_submitted": ("kyc_method",),
        "kyc_verification_completed": ("status", "review_type"),
        "account_opened": (),
        "card_activated": ("card_type",),
        "first_transaction_completed": ("currency", "transaction_type"),
    }
    for name in required_text[event["event_type"]]:
        _text(properties, name)
    if event["event_type"] == "profile_submitted" and not re.fullmatch(r"[A-Z]{2}", properties["country"]):
        raise EventValidationError("invalid_country", "country must contain two uppercase letters")
    if event["event_type"] == "kyc_verification_completed" and properties["status"] not in {"passed", "failed"}:
        raise EventValidationError("invalid_kyc_status", "Completed KYC status must be passed or failed")
    uuid_fields = {
        "account_opened": "account_id", "card_activated": "card_id",
        "first_transaction_completed": "transaction_id",
    }
    if event["event_type"] in uuid_fields:
        _uuid(properties, uuid_fields[event["event_type"]])
    if event["event_type"] == "first_transaction_completed":
        amount = properties.get("amount")
        try:
            valid_amount = type(amount) in (int, float) and math.isfinite(amount) and amount > 0
        except OverflowError:
            valid_amount = False
        if not valid_amount:
            raise EventValidationError("invalid_amount", "amount must be a finite positive number, not a boolean")
        if not re.fullmatch(r"[A-Z]{3}", properties["currency"]):
            raise EventValidationError("invalid_currency", "currency must contain three uppercase letters")

    try:
        canonical = json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        encoded = canonical.encode("utf-8")
    except (ValueError, UnicodeEncodeError, RecursionError) as exc:
        raise EventValidationError("invalid_json_value", "Event contains an unsupported number or Unicode value") from exc
    return ValidatedEvent(event, canonical, hashlib.sha256(encoded).hexdigest())
