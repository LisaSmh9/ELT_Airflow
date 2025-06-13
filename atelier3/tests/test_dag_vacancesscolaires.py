import pytest
from airflow.models import DagBag
from airflow.operators.python import PythonOperator
import os 

dags_path = os.path.join(os.path.dirname(__file__), '..', 'dags')

@pytest.fixture(scope="session")
def dag_bag():
    """
    Fixture réutilisable qui charge tous les DAGs une seule fois pour tous les tests.
    C'est plus efficace que de le faire dans chaque fonction de test.
    """
    # On initialise le DagBag en lui donnant le chemin de nos DAGs
    # et en ignorant les exemples d'Airflow pour un test plus propre.
    return DagBag(dag_folder=dags_path, include_examples=False)


def test_dag_bag_has_no_import_errors(dag_bag):
    """
    Vérifie qu'AUCUN fichier DAG n'a généré d'erreur à l'importation.
    """
    assert not dag_bag.import_errors, (
        f"Le DagBag a rencontré des erreurs lors de l'importation des fichiers DAGs: {dag_bag.import_errors}"
    )


def test_dag_is_loaded(dag_bag):
    """
    Teste que le DAG est bien présent dans le DagBag (après avoir vérifié les erreurs).
    """
    assert 'vacances_et_feries_dag' in dag_bag.dags, (
        "Le DAG 'vacances_et_feries_dag' n'a pas été trouvé. "
        f"DAGs qui ont été trouvés : {list(dag_bag.dags.keys())}"
    )


def test_task_existence(dag_bag):
    """
    Teste que la tâche 'extract_and_load_vacances' existe et est du bon type.
    """
    dag = dag_bag.dags['vacances_et_feries_dag']
    
    task = dag.get_task('extract_and_load_vacances')
    assert task is not None, "La tâche 'extract_and_load_vacances' n'a pas été trouvée."
    assert isinstance(task, PythonOperator)


def test_task_count(dag_bag):
    """
    Teste que le DAG contient exactement 1 tâche.
    """
    # CORRECTION: On accède directement au DAG via le dictionnaire.
    dag = dag_bag.dags['vacances_et_feries_dag']
    assert len(dag.tasks) == 1