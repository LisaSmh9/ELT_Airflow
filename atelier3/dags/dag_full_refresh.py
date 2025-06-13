# --- Import des librairies et modules ---
import logging
import os
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from utils.callbacks_modules import notify_failure, notify_success

load_dotenv()

# --- Fonctions exécutées par ce DAG ---

def delete_all_tables():
    """Supprime toutes les tables des sources Postgres et de la destination DuckDB."""
    logging.info("--- DÉBUT DE LA SUPPRESSION DE TOUTES LES TABLES ---")

    # Suppression des tables PostgreSQL
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_bronze")
        engine = pg_hook.get_sqlalchemy_engine()
        with engine.connect() as conn:
            logging.info("Suppression des tables PostgreSQL : temperatures, profil_coefficients, holidays...")
            conn.execute(text("DROP TABLE IF EXISTS temperatures;"))
            conn.execute(text("DROP TABLE IF EXISTS profil_coefficients;"))
            conn.execute(text("DROP TABLE IF EXISTS holidays;"))
            logging.info("Tables PostgreSQL supprimées avec succès.")
    except Exception as e:
        logging.error(f"Erreur lors de la suppression des tables PostgreSQL : {e}")
        raise

    # Suppression de la table DuckDB
    try:
        duckdb_conn_str = os.getenv("DATABASE_URL_gold")
        if duckdb_conn_str:
            engine_duck = create_engine(duckdb_conn_str)
            with engine_duck.connect() as conn:
                logging.info("Suppression de la table DuckDB : data_model_inputs...")
                conn.execute(text("DROP TABLE IF EXISTS data_model_inputs;"))
                logging.info("Table DuckDB supprimée avec succès.")
        else:
            logging.warning("Variable d'environnement DATABASE_URL_gold non trouvée, suppression ignorée.")
    except Exception as e:
        logging.error(f"Erreur lors de la suppression de la table DuckDB : {e}")
        raise
        
    logging.info("--- SUPPRESSION TERMINÉE ---")


def check_confirmation(**context):
    """Vérifie si la case de confirmation a été cochée dans l'interface."""
    if context["params"].get("confirm_full_refresh") is True:
        return "delete_tables_task"
    else:
        logging.info("Confirmation non cochée. Annulation de l'opération.")
        return "end_aborted"


# --- Définition du DAG de contrôle ---

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'on_failure_callback': notify_failure, 
}

with DAG(
    dag_id='pipeline_full_refresh',
    default_args=default_args,
    description="Lance un rafraîchissement complet et séquentiel de toute la chaîne de données.",
    schedule=None,  # Ne se lance que manuellement
    catchup=False,
    tags=['controller', 'pipeline', 'refresh'],
    params={
        "confirm_full_refresh": Param(
            type="boolean",
            default=False,
            title="Confirmer le rafraîchissement complet",
            description="ACTION IRRÉVERSIBLE. Si coché, toutes les tables seront supprimées et la pipeline sera rechargée séquentiellement.",
        )
    }
) as dag:

    start = DummyOperator(task_id='start')

    check_confirmation_branch = BranchPythonOperator(
        task_id='check_confirmation',
        python_callable=check_confirmation,
    )

    delete_tables = PythonOperator(
        task_id='delete_tables_task',
        python_callable=delete_all_tables,
    )
    
    # On définit un timeout généreux pour toutes les tâches qui attendent
    long_task_timeout = timedelta(hours=2)

    # Déclenchement séquentiel des DAGs métier
    trigger_vacances = TriggerDagRunOperator(
        task_id="trigger_dag_vacances",
        trigger_dag_id="vacances_et_feries_dag", 
        wait_for_completion=True,
        execution_timeout=long_task_timeout,
    )

    trigger_temperatures = TriggerDagRunOperator(
        task_id="trigger_on_cascade_dag_temperatures_profil_coeff_gold_table",
        trigger_dag_id="elt_temperature_pipeline",
        wait_for_completion=True,
        execution_timeout=long_task_timeout,
    )


    # Tâches de fin pour un graphe propre
    end_aborted = DummyOperator(task_id='end_aborted')
    
    end_success = DummyOperator(
        task_id='end_success',
        on_success_callback=notify_success,
        trigger_rule='all_success'
    )

    # --- Exécution ---
    
    start >> check_confirmation_branch
    
    # Chemin si l'utilisateur annule
    check_confirmation_branch >> end_aborted

    # Chemin séquentiel si l'utilisateur confirme
    check_confirmation_branch >> delete_tables
    #delete_tables >> trigger_vacances >> trigger_temperatures >> trigger_coefficients >> trigger_finale >> end_success
    delete_tables >> trigger_vacances >> trigger_temperatures >> end_success

