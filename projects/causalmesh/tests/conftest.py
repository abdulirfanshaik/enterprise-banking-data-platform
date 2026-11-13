import pytest

from causalmesh.engine import Engine
from causalmesh.storage import Store


@pytest.fixture
def engine(tmp_path):
    with Store(str(tmp_path / "test.db")) as store:
        yield Engine(store)
