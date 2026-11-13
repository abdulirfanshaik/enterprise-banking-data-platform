"""Executed with REAL PySpark in GitHub Actions; skipped without pyspark locally."""

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from causalmesh.cloud.contracts import event_schema  # noqa: E402
from causalmesh.cloud.pipeline import (  # noqa: E402
    as_of, build_gold, build_silver, validate_and_dedupe,
)
from causalmesh.cloud.job import run_snapshot  # noqa: E402
from causalmesh.cloud.contracts import StoragePaths  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .appName("CausalMeshSparkCI").getOrCreate())
    yield session
    session.stop()


def make_event(event_id, leg_id, revision, side, account, counterparty, amount, seq, t, transfer="T-1"):
    return dict(seq=seq, recorded_at=t, event_hash="a" * 64, prior_hash="0" * 64,
        event_id=event_id, leg_id=leg_id, transfer_id=transfer, revision=revision,
        side=side, account_id=account, counterparty_id=counterparty, currency="USD",
        amount_minor=amount, effective_at="2026-11-13T10:00:00+00:00", schema_version=1)


def read(spark, events):
    return spark.createDataFrame(events, schema=event_schema())


def fixture_rows():
    return [
        make_event("D1", "D", 1, "DEBIT", "A", "B", 1000, 1, "2026-11-13T10:01:00+00:00"),
        make_event("C1", "C", 1, "CREDIT", "B", "A", 1001, 2, "2026-11-13T10:02:00+00:00"),
        make_event("C2", "C", 2, "CREDIT", "B", "A", 1000, 3, "2026-11-13T10:15:00+00:00"),
    ]


def test_spark_temporal_truth_and_balanced_settlement(spark):
    bronze, invalid = validate_and_dedupe(read(spark, fixture_rows()))
    assert bronze.count() == 3 and invalid.count() == 0
    silver, broken = build_silver(bronze)
    assert broken.count() == 0 and silver.count() == 3
    old = as_of(silver, "2026-11-13T10:30:00+00:00", "2026-11-13T10:10:00+00:00")
    new = as_of(silver, "2026-11-13T10:30:00+00:00", "2026-11-13T10:20:00+00:00")
    assert old.filter("account_id = 'B'").collect()[0]["amount_minor"] == 1001
    assert new.filter("account_id = 'B'").collect()[0]["amount_minor"] == 1000
    transfers, balances = build_gold(silver)
    assert transfers.select("status").first()[0] == "SETTLED"
    d = {r["account_id"]: r["balance_minor"] for r in balances.collect()}
    assert d == {"A": -1000, "B": 1000}


def test_replay_dedupe_and_collision_quarantine(spark):
    duplicate = fixture_rows()[0]
    conflicting = dict(duplicate, amount_minor=9999, seq=5)
    rows = [duplicate, duplicate, conflicting, fixture_rows()[1]]
    bronze, rejected = validate_and_dedupe(read(spark, rows))
    assert bronze.count() == 1
    assert rejected.count() == 3
    assert set(r["reject_reason"] for r in rejected.collect()) == {"EVENT_ID_PAYLOAD_COLLISION"}


def test_revision_conflict_and_mismatched_settlement(spark):
    r = fixture_rows()[:2]
    r.append(make_event("bad", "C", 4, "CREDIT", "B", "A", 1000, 5,
                        "2026-11-13T10:15:00+00:00"))
    bronze, _ = validate_and_dedupe(read(spark, r))
    silver, issues = build_silver(bronze)
    assert issues.count() == 1
    transfers, _ = build_gold(silver)
    assert transfers.select("status").first()[0] == "MISMATCH"


def test_portable_parquet_end_to_end(spark, tmp_path):
    raw = read(spark, fixture_rows())
    output = str(tmp_path / "lake")
    metrics = run_snapshot(raw, StoragePaths(output), "parquet")
    assert metrics == {"accepted": 3, "quarantined": 0, "transfers": 1, "accounts": 2}
    transfers = spark.read.parquet(output + "/gold_transfers")
    assert transfers.first()["status"] == "SETTLED"
    assert spark.read.parquet(output + "/silver").count() == 3
