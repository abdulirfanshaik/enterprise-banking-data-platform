.PHONY: demo generate pipeline test clean kafka-up kafka-down

PYTHON ?= python3
PYTHONPATH := src

demo: generate pipeline

generate:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m banking_platform.generator --output data/raw --customers 250 --accounts 400 --transactions 3000

pipeline:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m banking_platform.local_pipeline --input data/raw --output data/processed

test:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m unittest discover -s tests -v

clean:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m banking_platform.clean --raw data/raw --processed data/processed

kafka-up:
	docker compose up -d kafka kafka-ui

kafka-down:
	docker compose down
