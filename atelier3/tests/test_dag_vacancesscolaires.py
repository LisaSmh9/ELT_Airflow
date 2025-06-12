import pytest
from airflow.models import DagBag
from airflow.operators.python import PythonOperator


def test_dag_import():
    """
    Teste que le DAG est bien importé sans erreur
    """
    dag_bag = DagBag()
    assert 'vacances_et_feries_dag' in dag_bag.dags
    assert dag_bag.import_errors == {}


def test_task_existence():
    """
    Teste que la tâche 'extract_and_load_vacances' existe et est du bon type
    """
    dag_bag = DagBag()
    dag = dag_bag.get_dag('vacances_et_feries_dag')

    task = dag.get_task('extract_and_load_vacances')
    assert isinstance(task, PythonOperator)


def test_task_count():
    """
    Teste que le DAG contient exactement 1 tâche
    """
    dag_bag = DagBag()
    dag = dag_bag.get_dag('vacances_et_feries_dag')
    assert len(dag.tasks) == 1
