from fastapi.testclient import TestClient

from causalmesh.api import create_app
from causalmesh.demo import sample_event
from causalmesh.engine import Engine
from causalmesh.storage import Store


def test_endpoints_and_ui(tmp_path):
    path = str(tmp_path / "api.db")
    with Store(path) as db:
        eng = Engine(db)
        eng.ingest(sample_event(31, "DEBIT"))
        eng.ingest(sample_event(31, "CREDIT"))
    client = TestClient(create_app(path))
    assert client.get("/healthz").json()["status"] == "ok"
    assert "Settlement command center" in client.get("/dashboard").text
    assert client.get("/api/overview").json()["events"] == 2
    assert client.get("/api/transfers/TRX-000031").json()["status"] == "SETTLED"
    assert client.get("/api/transfers/TRX-000099").status_code == 404
    assert client.get("/api/audit").json()["valid"]
    assert len(client.get("/api/lineage/TRX-000031").json()["sources"]) == 2
    assert "causalmesh_events 2" in client.get("/metrics").text
    assert "CausalMesh Control Plane" in client.get("/openapi.json").text


def test_counterfactual_api_and_validation(tmp_path):
    path = str(tmp_path / "api.db")
    with Store(path) as db:
        eng = Engine(db)
        eng.ingest(sample_event(32, "DEBIT"))
    client = TestClient(create_app(path))
    predicted = client.post("/api/simulate", json=sample_event(32, "CREDIT"))
    assert predicted.status_code == 200
    assert predicted.json()["after"]["status"] == "SETTLED"
    assert client.get("/api/overview").json()["events"] == 1
    assert client.post("/api/simulate", json={"bad": True}).status_code == 422
    assert client.get("/api/as-of/ACC-0001").status_code == 422
