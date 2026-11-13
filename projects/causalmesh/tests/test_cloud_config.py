from unittest.mock import Mock

import pytest

from causalmesh.cloud.job import JobConfig, register_unity_catalog


def test_kafka_requirements():
    with pytest.raises(ValueError, match="Kafka source"):
        JobConfig("unused", "s3://gold", "delta", "batch", "checkpoint", "kafka", "host", "topic")
    with pytest.raises(ValueError, match="Kafka source"):
        JobConfig("unused", "s3://gold", "delta", "stream", "checkpoint", "kafka")
    assert JobConfig("unused", "s3://gold", "delta", "stream", "checkpoint",
                     "kafka", "host:9092", "accepted").source == "kafka"


def test_unity_catalog_registration_identifiers_are_checked():
    spark = Mock()
    with pytest.raises(ValueError, match="identifier"):
        register_unity_catalog(spark, "s3://lake/delta", "prod;DROP", "finance")
    with pytest.raises(ValueError, match="safe S3"):
        register_unity_catalog(spark, "s3://bad'sql", "prod", "finance")
    register_unity_catalog(spark, "s3://bucket/delta", "prod", "finance")
    assert spark.sql.call_count == 5
    assert "`prod`.`finance`.`causalmesh_gold_transfers`" in str(spark.sql.call_args_list)
