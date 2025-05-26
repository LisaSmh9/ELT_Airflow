from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests
from datetime import datetime, timedelta
import io


def fetch_temperatures(beginning_date, end_date):
    base_url = "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/donnees-de-temperature-et-de-pseudo-rayonnement/records"
    all_data, offset, limit = [], 0, 100
    while True:
        params = {
            "where": f"horodate >= '{beginning_date}' AND horodate < '{end_date}'",
            "timezone": "Europe/Paris", "limit": limit, "offset": offset
        }
        response = requests.get(base_url, params=params)
        if response.status_code != 200:
            break
        data = response.json()
        results = data.get("results", [])
        all_data.extend(results)
        if len(results) < limit:
            break
        offset += limit
    return pd.DataFrame(all_data)


def update_temperature_table():
    hook = PostgresHook(postgres_conn_id="postgres_bronze")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Vérifier si la table existe
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
        cursor.execute("SELECT MAX(horodate) FROM temperatures;")
        result = cursor.fetchone()[0]
        beginning_date = result.date() if result else datetime(2023, 1, 1).date()

    df_all = pd.DataFrame()
    while beginning_date <= datetime.now().date():
        end_date = beginning_date + timedelta(days=1)
        df = fetch_temperatures(beginning_date, end_date)
        df_all = pd.concat([df_all, df], ignore_index=True)
        beginning_date = end_date

    if not df_all.empty:
        df_all["horodate"] = pd.to_datetime(df_all["horodate"], utc=True).dt.tz_convert("Europe/Paris")

        # Ajout des colonnes date (si pas déjà dans l'API)
        df_all["annee"] = df_all["horodate"].dt.year.astype(str)
        df_all["mois"] = df_all["horodate"].dt.month.astype(str).str.zfill(2)
        df_all["jour"] = df_all["horodate"].dt.day.astype(str).str.zfill(2)
        df_all["annee_mois_jour"] = df_all["horodate"].dt.strftime('%Y-%m-%d')

        if not table_exists:
            cursor.execute("""
                CREATE TABLE temperatures (
                    horodate TIMESTAMPTZ,
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

        # Préparer l'insertion via COPY (rapide et sûr)
        buffer = io.StringIO()
        df_all.to_csv(buffer, index=False, header=False, sep="\t", na_rep='\\N')
        buffer.seek(0)

        cursor.copy_expert("""
            COPY temperatures (
                horodate,
                temperature_realisee_lissee_degc,
                temperature_normale_lissee_degc,
                temperature_realisee_lissee_temperature_normale_lissee_degc,
                pseudo_rayonnement,
                annee,
                mois,
                jour,
                annee_mois_jour
            ) FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', NULL '\\N')
        """, buffer)
        conn.commit()

        # Index sur horodate si absent
        cursor.execute("""
            SELECT 1 FROM pg_indexes 
            WHERE tablename = 'temperatures' 
            AND indexname = 'idx_date';
        """)
        if not cursor.fetchone():
            cursor.execute("CREATE INDEX idx_date ON temperatures(horodate);")
            conn.commit()

        print("Températures insérées sans SQLAlchemy.")

    cursor.close()
    conn.close()


# === DAG Definition ===
with DAG(
    dag_id="elt_temperature_pipeline",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["elt", "temperatures"]
) as dag:

    update_temperatures_task = PythonOperator(
        task_id="update_temperature_table",
        python_callable=update_temperature_table,
    )
