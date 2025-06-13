# tests/test_ingest_parquet.py
import sys
import os
import pandas as pd
import pytest
from datetime import datetime

# 1) On ajoute le dossier 'dags' au PYTHONPATH
#    pour que "import ingest_parquet_with_hook" fonctionne.
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "dags")
    )
)

# 2) On importe la fonction à tester et ses constantes
from ingest_parquet_with_hook import ingest_multiple_parquets, PARQUET_FOLDER, TABLE_NAME

# --- Mocks pour PostgresHook ------------------------------------------------

class DummyCursor:
    def __init__(self):
        self._table_exists = False
        self._last = (None,)
    def execute(self, sql, params=None):
        if "SELECT to_regclass" in sql:
            # simule la réponse de to_regclass
            self._last = (TABLE_NAME,) if self._table_exists else (None,)
        elif sql.strip().upper().startswith("CREATE TABLE"):
            # on vient de créer la table
            self._table_exists = True
            self._last = (TABLE_NAME,)
    def fetchone(self):
        return self._last
    def fetchall(self):
        return []
    def copy_expert(self, sql, buf):
        # on ne fait rien : suffit que ça ne plante pas
        pass
    def close(self):
        pass

class DummyConn:
    def __init__(self):
        self.cur = DummyCursor()
    def cursor(self):
        return self.cur
    def commit(self):
        pass
    def close(self):
        pass

class DummyHook:
    def __init__(self, *args, **kwargs):
        pass
    def get_conn(self):
        return DummyConn()

# --- Fixtures ---------------------------------------------------------------

@pytest.fixture(autouse=True)
def patch_postgres_hook(monkeypatch):
    # On remplace PostgresHook dans ingest_parquet_with_hook par notre DummyHook
    monkeypatch.setattr(
        "ingest_parquet_with_hook.PostgresHook",
        lambda *args, **kwargs: DummyHook()
    )

@pytest.fixture
def tmp_folder(tmp_path, monkeypatch):
    # Crée un dossier vide et override PARQUET_FOLDER
    folder = tmp_path / "parquets"
    folder.mkdir()
    monkeypatch.setattr(
        "ingest_parquet_with_hook.PARQUET_FOLDER",
        str(folder)
    )
    return folder

# --- Tests ------------------------------------------------------------------

def test_ingest_no_files(tmp_folder):
    """Si aucun .parquet, on ne plante pas et on ne crée pas de .done."""
    ingest_multiple_parquets()
    assert list(tmp_folder.glob("*.done")) == []

def test_ingest_one_file(tmp_folder):
    """Un seul parquet génère un flag .done"""
    df = pd.DataFrame({
        "horodate": pd.date_range("2025-06-01", periods=2, freq="H"),
        "sous_profil": ["X", "Y"],
        "coefficient_ajuste": [0.1, 0.2],
        "coefficient_dynamique": [0.3, 0.4],
        "coefficient_prepare": [0.5, 0.6],
    })
    p = tmp_folder / "test.parquet"
    df.to_parquet(p)

    ingest_multiple_parquets()

    done = tmp_folder / "test.parquet.done"
    assert done.exists()

def test_idempotence(tmp_folder):
    """Réexécuter sur le même parquet ne doit pas planter"""
    df = pd.DataFrame({
        "horodate": pd.date_range("2025-06-02", periods=1, freq="H"),
        "sous_profil": ["Z"],
        "coefficient_ajuste": [0.7],
        "coefficient_dynamique": [0.8],
        "coefficient_prepare": [0.9],
    })
    p = tmp_folder / "one.parquet"
    df.to_parquet(p)

    # première exécution → flag créé
    ingest_multiple_parquets()
    assert (tmp_folder / "one.parquet.done").exists()

    # on supprime et on relance → doit recréer le flag
    os.remove(tmp_folder / "one.parquet.done")
    ingest_multiple_parquets()
    assert (tmp_folder / "one.parquet.done").exists()
