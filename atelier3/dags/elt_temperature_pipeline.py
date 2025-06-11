# =============================================================================
#                           IMPORT LIBRAIRIES ET MODULES
# =============================================================================
from airflow.datasets import Dataset
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import requests
from datetime import datetime, timedelta
import io
from utils.callbacks_modules import notify_failure, notify_success
import logging
print("Callback notify_failure:", notify_failure)
print("Callback notify_success:", notify_success)




# -- Définition du Dataset --
TEMPERATURE_TABLE_DATASET = Dataset("table_temperatures")

# =============================================================================
#                            LOGIQUE DE LA TÂCHE (ETL)
# =============================================================================

def fetch_temperatures(beginning_date, end_date):
    """
    Récupère les données de température depuis l'API Enedis.
    """
    base_url = "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/donnees-de-temperature-et-de-pseudo-rayonnement/records"
    all_data, offset, limit = [], 0, 100
    
    print(f"Récupération des données pour la période : {beginning_date} à {end_date}")

    while True:
        params = {
            "where": f"horodate >= '{beginning_date}' AND horodate < '{end_date}'",
            "timezone": "Europe/Paris", "limit": limit, "offset": offset
        }
        try:
            response = requests.get(base_url, params=params)
            
            # GESTION DES ERREURS HTTP
            # Si le statut n'est pas 200 (ex: 404, 500), lève une exception et arrête tout.
            response.raise_for_status()

            data = response.json()
            results = data.get("results", [])
            all_data.extend(results)

            if len(results) < limit:
                break
            offset += limit
        except requests.exceptions.RequestException as e:
            print(f"Erreur critique lors de l'appel à l'API Enedis : {e}")
            raise # On propage l'erreur pour faire échouer la tâche Airflow

    # GESTION DES RÉPONSES VIDES
    # Si la boucle se termine mais qu'aucune donnée n'a été collectée, on considère cela
    # comme une erreur et on arrête la tâche.
    if not all_data:
        raise ValueError(f"Aucune donnée retournée par l'API pour la période du {beginning_date}.")

    print(f"{len(all_data)} enregistrements trouvés.")
    return pd.DataFrame(all_data)


def update_temperature_table():
    hook = PostgresHook(postgres_conn_id="postgres_bronze")
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'temperatures'
        );
    """)
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        beginning_date = datetime(2023, 1, 1).date()
    else:
        cursor.execute("SELECT MAX(horodate) FROM temperatures WHERE horodate IS NOT NULL;")
        result = cursor.fetchone()[0]
        beginning_date = (result + timedelta(days=1)).date() if result else datetime(2023, 1, 1).date()
    
    jour_a_traiter_limite = datetime.now().date()
    df_all = pd.DataFrame()
    
    while beginning_date < jour_a_traiter_limite:
        end_date = beginning_date + timedelta(days=1)
        df = fetch_temperatures(beginning_date, end_date)
        df_all = pd.concat([df_all, df], ignore_index=True)
        beginning_date = end_date

    if not df_all.empty:
        df_all["horodate"] = pd.to_datetime(df_all["horodate"], utc=True).dt.tz_convert("Europe/Paris")
        df_all["annee"] = df_all["horodate"].dt.year.astype(str)
        df_all["mois"] = df_all["horodate"].dt.month.astype(str).str.zfill(2)
        df_all["jour"] = df_all["horodate"].dt.day.astype(str).str.zfill(2)
        df_all["annee_mois_jour"] = df_all["horodate"].dt.strftime('%Y-%m-%d')

        if not table_exists:
            cursor.execute("""
                CREATE TABLE temperatures (
                    horodate TIMESTAMPTZ PRIMARY KEY,
                    temperature_realisee_lissee_degc FLOAT,
                    temperature_normale_lissee_degc FLOAT,
                    temperature_realisee_lissee_temperature_normale_lissee_degc FLOAT,
                    pseudo_rayonnement FLOAT,
                    annee TEXT,
                    mois TEXT,
                    jour TEXT,
                    annee_mois_jour TEXT
                );
            """)
            conn.commit()

        buffer = io.StringIO()
        df_all.to_csv(buffer, index=False, header=False, sep="\t", na_rep='\\N')
        buffer.seek(0)
        cursor.copy_expert("""
            COPY temperatures FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')
        """, buffer)
        conn.commit()

        cursor.execute("SELECT 1 FROM pg_indexes WHERE tablename = 'temperatures' AND indexname = 'idx_date';")
        if not cursor.fetchone():
            cursor.execute("CREATE INDEX idx_date ON temperatures(horodate);")
            conn.commit()
        print(f"{len(df_all)} lignes de températures insérées.")
    else:
        print("Aucune nouvelle donnée de température à insérer.")

    cursor.close()
    conn.close()



# =============================================================================
#                            DÉFINITION DU DAG
# =============================================================================
with DAG(
    dag_id="elt_temperature_pipeline",
    start_date=days_ago(1),
    schedule_interval="0 8 * * *",
    catchup=False,
    tags=["elt", "temperatures", "producteur"],
    
) as dag:
    update_temperatures_task = PythonOperator(
        task_id="update_temperature_table",
        python_callable=update_temperature_table,
        outlets=[TEMPERATURE_TABLE_DATASET],
        on_failure_callback=notify_failure,
        #on_success_callback=notify_success,
    )