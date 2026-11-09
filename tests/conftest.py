import pytest


@pytest.fixture()
def data_dir(tmp_path, monkeypatch):
    """Chaque test travaille dans son propre dossier de données."""
    monkeypatch.setenv("NETPULSE_DATA_DIR", str(tmp_path))
    return tmp_path


@pytest.fixture(scope="session")
def spark():
    from netpulse.transform import get_spark

    s = get_spark("netpulse-tests")
    yield s
    s.stop()
