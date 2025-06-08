
# Import librairies
import logging
import os
import io 
from datetime import datetime, timedelta
from airflow.datasets import Dataset
import pandas as pd
import pyarrow.dataset as ds
from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyarrow.lib import ArrowInvalid
from sqlalchemy import MetaData, Table, text, create_engine
from sqlalchemy.types import TIMESTAMP
from dotenv import load_dotenv
from utils.callbacks_modules import notify_failure

load_dotenv()

# Création des datasets
TEMPERATURE_TABLE_DATASET = Dataset("table_temperatures")
COEFFICIENTS_TABLE_DATASET = Dataset("table_profil_coefficients")

# Déclaration des variables
POSTGRES_CONN_ID = "postgres_bronze"
PARQUET_PATH = os.getenv('DATA_DIR')
TABLE_NAME = "profil_coefficients"
CHUNK_SIZE = 10000 
TEMP_TABLE_NAME = f"temp_{TABLE_NAME}_upsert"

def get_engine() -> create_engine:
    """Crée et retourne une engine SQLAlchemy via le hook Airflow."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    return hook.get_sqlalchemy_engine()

def table_exists(engine: create_engine, table_name: str) -> bool:
    """Vérifie si une table existe."""
    with engine.connect() as conn:
        return engine.dialect.has_table(conn, table_name)

# --- Chargement Initial ---

def task_create_table():
    """Tâche Airflow : Crée la table cible à partir du schéma du Parquet."""
    engine = get_engine()
    if table_exists(engine, TABLE_NAME):
        logging.info(f"La table {TABLE_NAME} existe déjà. Création annulée.")
        return

    logging.info(f"Création de la table {TABLE_NAME} à partir de {PARQUET_PATH}.")
    try:
        dataset = ds.dataset(PARQUET_PATH, format="parquet")
        df_sample = next(dataset.to_batches(batch_size=10)).to_pandas()
    except (StopIteration, ArrowInvalid) as e:
        logging.error(f"Impossible de lire le fichier Parquet pour en déduire le schéma: {e}")
        raise

    if 'horodate' in df_sample.columns:
        df_sample['horodate'] = pd.to_datetime(df_sample['horodate'], utc=True)
        sql_types = {'horodate': TIMESTAMP(timezone=True)}
    else:
        sql_types = None

    logging.info("Création de la structure de la table...")
    df_sample.head(0).to_sql(TABLE_NAME, engine, index=False, dtype=sql_types)
    with engine.connect() as conn:
        conn.execute(text(f'ALTER TABLE {TABLE_NAME} ADD PRIMARY KEY (identifiant, horodate);'))
        conn.commit()
    logging.info(f"Table {TABLE_NAME} créée avec succès et clé primaire ajoutée.")


def task_initial_load():
    """Tâche Airflow : Charge l'intégralité du fichier Parquet."""
    engine = get_engine()
    logging.info(f"Début du chargement initial complet depuis {PARQUET_PATH}...")
    dataset = ds.dataset(PARQUET_PATH, format="parquet")
    total_rows = 0
    for batch in dataset.to_batches(batch_size=CHUNK_SIZE):
        df = batch.to_pandas()
        if 'horodate' in df.columns:
            df['horodate'] = pd.to_datetime(df['horodate'], utc=True)
        df.to_sql(TABLE_NAME, engine, if_exists='append', index=False, method='multi')
        total_rows += len(df)
        logging.info(f"{total_rows} lignes insérées...")
    logging.info(f"Chargement initial terminé. Total de {total_rows} lignes insérées.")

# --- Mise à jour incrémentale ---

def _get_changed_data(engine: create_engine) -> pd.DataFrame:
    """Compare les données récentes du Parquet et de la DB pour trouver les deltas."""
    from datetime import datetime, timedelta
    
    logging.info("Début de l'identification des changements.")
    seven_days_ago = datetime.utcnow() - timedelta(days=7)
    
    logging.info("Chargement des 7 derniers jours depuis le Parquet...")
    dataset = ds.dataset(PARQUET_PATH, format="parquet")
    try:
        filter_date_berlin = pd.Timestamp(seven_days_ago, tz="UTC").tz_convert('Europe/Berlin')
        df_parquet = dataset.to_table(filter=(ds.field("horodate") >= filter_date_berlin)).to_pandas()
        if 'horodate' in df_parquet.columns:
            df_parquet['horodate'] = pd.to_datetime(df_parquet['horodate'], utc=True)
        logging.info(f"{len(df_parquet)} lignes récentes trouvées dans le Parquet.")
    except (ArrowInvalid, StopIteration):
        logging.warning("Aucune donnée récente dans le fichier Parquet.")
        return pd.DataFrame()

    if df_parquet.empty:
        return pd.DataFrame()

    logging.info("Chargement des données correspondantes depuis la base de données...")
    min_date = df_parquet['horodate'].min()
    query = text(f"SELECT * FROM {TABLE_NAME} WHERE horodate >= :min_date")
    df_db = pd.read_sql(query, engine, params={'min_date': min_date}, parse_dates=['horodate'])
    if 'horodate' in df_db.columns and not df_db.empty:
        df_db['horodate'] = df_db['horodate'].dt.tz_convert('UTC')
    
    logging.info(f"{len(df_db)} lignes correspondantes trouvées dans la base.")
    if df_db.empty:
        return df_parquet

    key_cols = ['identifiant', 'horodate']
    value_cols = sorted([col for col in df_parquet.columns if col not in key_cols])

    if not value_cols:
        df_merged = df_parquet.merge(df_db[key_cols], on=key_cols, how='left', indicator=True)
        df_to_upsert = df_merged[df_merged['_merge'] == 'left_only'][df_parquet.columns]
    else:
        # <-- OPTIMISATION 1: Calcul du hash vectorisé, beaucoup plus rapide que .apply()
        logging.info("Calcul des hashs de comparaison (méthode vectorisée)...")
        cols_as_str_parquet = df_parquet[value_cols].astype(str)
        df_parquet['hash'] = pd.util.hash_pandas_object(cols_as_str_parquet, index=False)

        cols_as_str_db = df_db[value_cols].astype(str)
        df_db['hash'] = pd.util.hash_pandas_object(cols_as_str_db, index=False)
        
        df_merged = df_parquet.merge(df_db, on=key_cols, how='left', suffixes=('_parquet', '_db'))
        changed_rows_df = df_merged[
            (df_merged['hash_db'].isna()) | (df_merged['hash_parquet'] != df_merged['hash_db'])
        ]
        df_to_upsert = changed_rows_df[df_parquet.columns.drop('hash')]

    logging.info(f"Comparaison terminée. {len(df_to_upsert)} lignes à insérer/mettre à jour.")
    return df_to_upsert

# -- upsert en utilisant la fonction COPY pour une meilleure optimisation
def _perform_upsert(df_to_upsert: pd.DataFrame, engine: create_engine):
    """Charge un DataFrame dans la table temp via COPY (très rapide) et exécute un upsert."""
    if df_to_upsert.empty:
        logging.info("Aucune donnée à mettre à jour.")
        return

    conn = engine.raw_connection()
    try:
        with conn.cursor() as cursor:
            # 1. Créer une table temporaire. ON COMMIT PRESERVE ROWS est important.
            cursor.execute(f"CREATE TEMP TABLE {TEMP_TABLE_NAME} (LIKE {TABLE_NAME}) ON COMMIT PRESERVE ROWS;")
            logging.info(f"Table temporaire {TEMP_TABLE_NAME} créée.")

            # 2. Convertir le DataFrame en un fichier CSV en mémoire
            s_buf = io.StringIO()
            # Utiliser la tabulation comme séparateur pour éviter les problèmes avec les virgules dans les données
            df_to_upsert.to_csv(s_buf, header=False, index=False, sep='\t')
            s_buf.seek(0)

            # 3. charger des données dans PostgreSQL
            logging.info(f"Chargement de {len(df_to_upsert)} lignes via COPY dans la table temporaire...")
            cursor.copy_expert(f"COPY {TEMP_TABLE_NAME} FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t')", s_buf)
            logging.info("Chargement en table temporaire terminé.")

            # 4. Construire et exécuter la requête d'UPSERT en masse
            metadata = MetaData()
            target_table = Table(TABLE_NAME, metadata, autoload_with=engine)
            key_cols = ['identifiant', 'horodate']
            update_cols = [col.name for col in target_table.columns if col.name not in key_cols]
            set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
            
            upsert_query = text(f"""
                INSERT INTO {TABLE_NAME}
                SELECT * FROM {TEMP_TABLE_NAME}
                ON CONFLICT (identifiant, horodate)
                DO UPDATE SET {set_clause};
            """)
            
            logging.info("Exécution de la requête d'UPSERT en masse...")
            # On utilise une nouvelle connexion du pool pour exécuter cette requête
            with engine.connect() as sql_conn:
                sql_conn.execute(upsert_query)
                sql_conn.commit()
            
            logging.info("Upsert terminé.")
        
        # Le commit final de la transaction principale
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Erreur pendant l'upsert, transaction annulée : {e}")
        raise
    finally:
        conn.close()


def task_incremental_update():
    """Tâche Airflow : Identifie les changements et les applique."""
    engine = get_engine()
    df_to_upsert = _get_changed_data(engine)
    _perform_upsert(df_to_upsert, engine)


def _check_if_table_exists():
    """Fonction de branchement : choisit le chemin en fonction de l'existence de la table."""
    engine = get_engine()
    if table_exists(engine, TABLE_NAME):
        return 'run_incremental_update'
    else:
        return 'run_initial_load_branch'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'depends_on_past': False,
}

with DAG(
    dag_id='update_coefficient_profil', 
    default_args=default_args,
    schedule=[TEMPERATURE_TABLE_DATASET],
    catchup=False,
    tags=['profil', 'upsert'],
    doc_md="""
    ### DAG de synchronisation des profils (VERSION OPTIMISÉE)
    - Utilise un hash vectorisé pour la comparaison.
    - Utilise COPY pour une écriture très rapide en base de données.
    """,
    on_failure_callback=notify_failure
) as dag:

    start = DummyOperator(task_id='start')
    check_table_exists = BranchPythonOperator(task_id='check_if_table_exists', python_callable=_check_if_table_exists)
    run_initial_load_branch = DummyOperator(task_id='run_initial_load_branch')
    create_table = PythonOperator(task_id='create_table_if_not_exists', python_callable=task_create_table)
    initial_load = PythonOperator(task_id='initial_load_all_data', python_callable=task_initial_load)
    incremental_update = PythonOperator(task_id='run_incremental_update', python_callable=task_incremental_update)
    end = DummyOperator(task_id='end', 
                        trigger_rule='none_failed_min_one_success',
                        outlets=[COEFFICIENTS_TABLE_DATASET])

    start >> check_table_exists
    check_table_exists >> run_initial_load_branch >> create_table >> initial_load >> end
    check_table_exists >> incremental_update >> end
