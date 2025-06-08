# Import librairies
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from utils.callbacks_modules import notify_failure, notify_success
from airflow.datasets import Dataset
load_dotenv() 

# Création des datasets pour le wait
TEMPERATURE_TABLE_DATASET = Dataset("table_temperatures")
COEFFICIENTS_TABLE_DATASET = Dataset("table_profil_coefficients")


beginning_date_str = "2023-01-01"

def transform_and_load_to_duckdb():
    # Connexion PostgreSQL
    hook = PostgresHook(postgres_conn_id="postgres_bronze")
    holidays_df = hook.get_pandas_df(f"SELECT * FROM holidays WHERE date >= '{beginning_date_str}'")
    temps_df = hook.get_pandas_df(f"SELECT * FROM temperatures WHERE horodate >= '{beginning_date_str}'")
    coeffs_df = hook.get_pandas_df(f"SELECT * FROM profil_coefficients WHERE horodate >= '{beginning_date_str}'")

    
    # Transformation des horodate sur le même fuseau
    temps_df['horodate'] = pd.to_datetime(temps_df['horodate']).dt.tz_convert('UTC').dt.tz_localize(None)
    coeffs_df['horodate'] = pd.to_datetime(coeffs_df['horodate']).dt.tz_convert('UTC').dt.tz_localize(None)


    # Fusion des tables
    df = coeffs_df.merge(temps_df, on="horodate", how="left")
    
    # Création colonne date_only pour faciliter la jointure
    df["date_only"] = df["horodate"].dt.date
    df = df.merge(holidays_df, left_on="date_only", right_on="date", how="left")
    df.drop(columns=["date_only"], inplace=True)


    # Calcul des champs temporels
    df['timestamp'] = pd.to_datetime(df['horodate'])
    df["day_of_week"] = df["timestamp"].dt.weekday
    df["day_of_year"] = df["timestamp"].dt.dayofyear
    df["half_hour"] = df["timestamp"].dt.hour * 2 + df["timestamp"].dt.minute // 30

    # fr_holiday 
    df["fr_holiday"] = df[["vacances_zone_a", "vacances_zone_b", "vacances_zone_c"]].fillna(0).astype(int).astype(str).agg("".join, axis=1)

    # is_public_holiday
    df["is_public_holiday"] = df["is_public_holiday"].fillna(False).astype(bool)

    # Sélection des colonnes finales
    final_columns = ['timestamp', 'temperature_realisee_lissee_degc', 'temperature_normale_lissee_degc', 'coefficient_prepare', 
                     "sous_profil","day_of_week", "day_of_year", "half_hour","fr_holiday", "is_public_holiday"]
    
    df_final = df[final_columns]
    
    df_final = df_final.rename(columns = {'temperature_realisee_lissee_degc': 'trl', 
                               'temperature_normale_lissee_degc' : 'tnl', 
                               'coefficient_prepare' : 'cp'})

    # Connexion DuckDB via SQLAlchemy
    duckdb_conn_str = os.getenv("DATABASE_URL_gold")
    engine = create_engine(duckdb_conn_str)

    # Écriture dans DuckDB
    df_final.to_sql("data_model_inputs", con=engine, if_exists="replace", index=False)

# Arguments par défaut du DAG
default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# Définition du DAG
with DAG(
    'transform_postgres_to_duckdb',
    default_args=default_args,
    schedule=[TEMPERATURE_TABLE_DATASET, COEFFICIENTS_TABLE_DATASET],
    catchup=False,
    description="DAG to transform data from Postgres and load into DuckDB",
    on_success_callback=notify_success,
    on_failure_callback=notify_failure,
) as dag:
    transform_task = PythonOperator(
        task_id='transform_and_load_to_duckdb',
        python_callable=transform_and_load_to_duckdb
    )

