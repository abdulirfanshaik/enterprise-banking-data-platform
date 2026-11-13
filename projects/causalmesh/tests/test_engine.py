from causalmesh.demo import populate, sample_event

T0 = "2026-11-13T10:00:00+00:00"
T1 = "2026-11-13T10:05:00+00:00"
T2 = "2026-11-13T10:15:00+00:00"
T3 = "2026-11-13T10:30:00+00:00"


def put(engine, n, side, amount=1000, revision=1, at=T0, received=T1):
    return engine.ingest(sample_event(n, side, amount, revision, at), recorded_at=received)


def test_mirrored_legs_settle_in_either_order(engine):
    assert put(engine, 1, "CREDIT")["status"] == "accepted"
    assert engine.reconciliation("TRX-000001")["status"] == "PENDING"
    assert put(engine, 1, "DEBIT")["status"] == "accepted"
    assert engine.reconciliation("TRX-000001")["status"] == "SETTLED"
    assert engine.metrics()["transfers"]["SETTLED"] == 1


def test_mismatched_amounts_are_flagged(engine):
    put(engine, 2, "DEBIT", 1000)
    put(engine, 2, "CREDIT", 1001)
    assert engine.reconciliation("TRX-000002")["status"] == "MISMATCH"
    rules = [r[0] for r in engine.db.conn.execute("SELECT rule FROM alerts")]
    assert "UNBALANCED_SETTLEMENT" in rules


def test_correction_maintains_bitemporal_truth(engine):
    put(engine, 3, "DEBIT", 1000, received=T1)
    put(engine, 3, "CREDIT", 1001, received=T1)
    assert put(engine, 3, "CREDIT", 1000, revision=2, received=T2)["status"] == "accepted"
    account = sample_event(3, "CREDIT")["account_id"]
    historical = engine.balance_as_of(account, T3, "2026-11-13T10:10:00+00:00")
    current = engine.balance_as_of(account, T3, T3)
    assert historical["balances_minor"]["USD"] == 1001
    assert current["balances_minor"]["USD"] == 1000
    assert engine.reconciliation("TRX-000003")["status"] == "SETTLED"
    assert "UNBALANCED_SETTLEMENT" not in [r[0] for r in engine.db.conn.execute(
        "SELECT rule FROM alerts WHERE transfer_id='TRX-000003'"
    )]


def test_business_clock_is_independent_from_knowledge_clock(engine):
    put(engine, 4, "DEBIT", 500, at=T2, received=T1)
    account = sample_event(4, "DEBIT")["account_id"]
    assert engine.balance_as_of(account, T0, T3)["balances_minor"] == {}
    assert engine.balance_as_of(account, T3, T0)["balances_minor"] == {}
    assert engine.balance_as_of(account, T3, T3)["balances_minor"]["USD"] == -500


def test_idempotency_and_conflict_dlq(engine):
    original = sample_event(5, "DEBIT", 500, effective_at=T0)
    assert engine.ingest(original, T1)["status"] == "accepted"
    assert engine.ingest(original, T2)["status"] == "duplicate"
    conflict = {**original, "amount_minor": 777}
    assert engine.ingest(conflict, T2)["status"] == "quarantined"
    assert engine.metrics()["events"] == 1
    assert engine.metrics()["quarantined"] == 1
    assert engine.verify_audit()["valid"]


def test_revision_gap_goes_to_quarantine_without_mutation(engine):
    assert put(engine, 6, "DEBIT", revision=2)["status"] == "quarantined"
    assert engine.metrics()["events"] == 0
    put(engine, 6, "DEBIT", revision=1)
    assert put(engine, 6, "DEBIT", revision=3)["status"] == "quarantined"
    assert engine.metrics()["events"] == 1


def test_identity_cannot_change_on_revision(engine):
    put(engine, 7, "DEBIT")
    changed = sample_event(7, "DEBIT", revision=2, effective_at=T0)
    changed["account_id"] = "DIFFERENT"
    assert engine.ingest(changed, T2)["status"] == "quarantined"
    assert engine.metrics()["active_legs"] == 1


def test_extra_leg_is_quarantined(engine):
    put(engine, 8, "DEBIT")
    put(engine, 8, "CREDIT")
    third = sample_event(8, "CREDIT", effective_at=T0)
    third.update(event_id="third", leg_id="third", account_id="OTHER")
    assert engine.ingest(third, T2)["status"] == "quarantined"


def test_audit_proof_detects_corruption(engine):
    put(engine, 9, "DEBIT")
    put(engine, 9, "CREDIT")
    assert engine.verify_audit()["checked"] == 2
    engine.db.conn.execute("UPDATE raw_events SET payload_json='{}' WHERE seq=1")
    result = engine.verify_audit()
    assert result["valid"] is False
    assert result["failed_seq"] == 1


def test_replay_is_idempotent_and_preserves_temporal_queries(engine):
    for n in range(10, 15):
        put(engine, n, "DEBIT")
        put(engine, n, "CREDIT", 1010 if n == 11 else 1000)
    put(engine, 11, "CREDIT", 1000, revision=2, received=T2)
    before = engine.metrics()
    old = engine.balance_as_of(sample_event(11, "CREDIT")["account_id"], T3, T1)
    assert engine.replay()["replayed"] == 11
    assert engine.metrics() == before
    assert engine.replay()["replayed"] == 11
    after = engine.balance_as_of(sample_event(11, "CREDIT")["account_id"], T3, T1)
    assert old == after


def test_counterfactual_is_non_persistent(engine):
    put(engine, 15, "DEBIT", 2000)
    put(engine, 15, "CREDIT", 2500)
    before = engine.metrics()
    imagined = sample_event(15, "CREDIT", 2000, revision=2, effective_at=T0)
    output = engine.simulate(imagined)
    assert output["before"]["status"] == "MISMATCH"
    assert output["after"]["status"] == "SETTLED"
    assert output["committed"] is False
    assert engine.metrics() == before
    assert engine.reconciliation("TRX-000015")["status"] == "MISMATCH"


def test_lineage_evidence_includes_all_revisions(engine):
    put(engine, 16, "DEBIT")
    put(engine, 16, "DEBIT", 800, revision=2, received=T2)
    trace = engine.lineage("TRX-000016")
    assert len(trace["sources"]) == 2
    assert trace["sources"][0]["event_hash"]


def test_late_and_high_value_rules(engine):
    assert put(engine, 17, "DEBIT", 7_000_000, received=T2)["status"] == "accepted"
    rules = [row[0] for row in engine.db.conn.execute("SELECT rule FROM alerts")]
    assert "HIGH_VALUE" in rules
    assert "LATE_ARRIVAL" not in rules  # exactly 15 mins threshold; strictly >
    assert put(engine, 18, "DEBIT", 1000, received=T3)["status"] == "accepted"
    rules = [row[0] for row in engine.db.conn.execute("SELECT rule FROM alerts")]
    assert "LATE_ARRIVAL" in rules


def test_simulation_invalid_does_not_persist(engine):
    bad = sample_event(19, "DEBIT", revision=3, effective_at=T0)
    import pytest
    with pytest.raises(ValueError):
        engine.simulate(bad)
    assert engine.metrics()["events"] == 0


def test_synthetic_chaos_demo_has_all_expected_states(engine):
    report = populate(engine, 125, seed=10)
    assert report["ingestion"]["accepted"] > 200
    assert report["ingestion"]["duplicate"] > 0
    assert report["ingestion"]["quarantined"] > 0
    assert report["metrics"]["transfers"]["SETTLED"] > 0
    assert report["metrics"]["transfers"]["MISMATCH"] > 0
    assert engine.verify_audit()["valid"]
    assert engine.replay()["audit"]["valid"]
