"""Local defaults match compose.yaml; environment variables override them."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    redis_url: str
    fraud_target: str

    @classmethod
    def from_env(cls):
        return cls(
            database_url=os.getenv(
                "DATABASE_URL",
                "postgresql://sentinel:sentinel_local@127.0.0.1:55433/sentinelpay",
            ),
            redis_url=os.getenv("REDIS_URL", "redis://127.0.0.1:56379/0"),
            fraud_target=os.getenv("FRAUD_TARGET", "127.0.0.1:50051"),
        )
