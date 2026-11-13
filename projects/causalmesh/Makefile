.PHONY: install demo api test lint audit replay lake stream
install:
	python -m pip install -e '.[dev]'
demo:
	causalmesh demo --transfers 250
api:
	uvicorn causalmesh.api:app --reload --host 127.0.0.1 --port 8000
test:
	pytest -q
lint:
	ruff check .
audit:
	causalmesh audit
replay:
	causalmesh replay
lake:
	causalmesh export
stream:
	docker compose --profile stream up -d redpanda

cloud-test:
	pytest -q tests/test_cloud_export.py tests/test_cloud_config.py tests/test_cloud_spark.py

cloud-demo:
	bash scripts/cloud_demo.sh
