from __future__ import annotations

from dataclasses import dataclass
import os

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class SalesforceConfig:
    username: str
    password: str
    security_token: str
    domain: str = "test"
    watermark: str = "1970-01-01T00:00:00Z"

    @classmethod
    def from_env(cls) -> "SalesforceConfig":
        return cls(
            username=os.getenv("SF_USERNAME", ""),
            password=os.getenv("SF_PASSWORD", ""),
            security_token=os.getenv("SF_SECURITY_TOKEN", ""),
            domain=os.getenv("SF_DOMAIN", "test"),
            watermark=os.getenv("SF_WATERMARK", "1970-01-01T00:00:00Z"),
        )


@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str
    role: str

    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        return cls(
            account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
            user=os.getenv("SNOWFLAKE_USER", ""),
            password=os.getenv("SNOWFLAKE_PASSWORD", ""),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "DE_WH"),
            database=os.getenv("SNOWFLAKE_DATABASE", "PORTFOLIO_DE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW_SALESFORCE"),
            role=os.getenv("SNOWFLAKE_ROLE", "DE_PIPELINE_ROLE"),
        )
