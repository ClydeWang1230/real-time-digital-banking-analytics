"""Shared runtime settings. Loading settings never opens Kafka or a database."""
#统一的 “设置表” —— kafka位置，消息发到哪个主题等等
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
from typing import Mapping


PROJECT_ROOT = Path(__file__).resolve().parent
SQL_DIR = PROJECT_ROOT / "sql"


@dataclass(frozen=True) #类
class Settings:#设置变量时也规定类型
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "banking-events-analytics"
    kafka_group_id: str = "banking-analytics-group-v2"
    kafka_auto_offset_reset: str = "earliest"
    db_path: Path = PROJECT_ROOT / "banking_events_analytics.duckdb"
    customer_count: int = 100
    delivery_timeout_seconds: int = 30
    queue_timeout_seconds: int = 10
    kafka_timeout_seconds: int = 10


def load_settings(environ: Mapping[str, str] | None = None) -> Settings:
    """Read environment overrides; relative database paths start at the repo root."""
    env = os.environ if environ is None else environ
    defaults = Settings()

    def value(name: str, default: str) -> str:
        result = env.get(name, default).strip()
        if not result:
            raise ValueError(f"{name} must not be empty.")
        return result

    def positive_integer(name: str, default: int) -> int:
        try:
            result = int(value(name, str(default)))
        except ValueError as exc:
            raise ValueError(f"{name} must be a positive integer.") from exc
        if result <= 0:
            raise ValueError(f"{name} must be a positive integer.")
        return result

    customer_count = positive_integer("BANKING_CUSTOMER_COUNT", defaults.customer_count)

    db_path = Path(value("BANKING_DB_PATH", str(defaults.db_path))).expanduser()
    if not db_path.is_absolute():
        db_path = PROJECT_ROOT / db_path

    return Settings(
        kafka_bootstrap_servers=value(
            "BANKING_KAFKA_BOOTSTRAP_SERVERS", defaults.kafka_bootstrap_servers
        ),
        kafka_topic=value("BANKING_KAFKA_TOPIC", defaults.kafka_topic),
        kafka_group_id=value("BANKING_KAFKA_GROUP_ID", defaults.kafka_group_id),
        db_path=db_path.resolve(),
        customer_count=customer_count,
        delivery_timeout_seconds=positive_integer("BANKING_DELIVERY_TIMEOUT_SECONDS", defaults.delivery_timeout_seconds),
        queue_timeout_seconds=positive_integer("BANKING_QUEUE_TIMEOUT_SECONDS", defaults.queue_timeout_seconds),
        kafka_timeout_seconds=positive_integer("BANKING_KAFKA_TIMEOUT_SECONDS", defaults.kafka_timeout_seconds),
    )


def main() -> None:
    """Display effective settings without connecting to external services."""
    print(json.dumps(asdict(load_settings()), indent=2, default=str))


if __name__ == "__main__":
    main()
