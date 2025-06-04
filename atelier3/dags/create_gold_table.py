from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv() 

def transform_and_load_to_duckdb():
    # Connexion PostgreSQL
    hook = PostgresHook(postgres_conn_id="postgres_bronze")
    holidays_df = hook.get_pandas_df("SELECT * FROM holidays")
    temps_df = hook.get_pandas_df("SELECT * FROM temperatures")
    coeffs_df = hook.get_pandas_df("SELECT * FROM profil_coefficients")

    
    # conn = hook.get_conn()
    # cursor = conn.cursor()
    
    # # Extraction des données PostgreSQL
    # cursor.execute("""
    #     SELECT * FROM holidays
    #     ;
    # """)
    # holidays_df = cursor.fetchone()[0]
    
    # cursor.execute("""
    #     SELECT * FROM temperatures
    #     ;
    # """)
    # temps_df = cursor.fetchone()[0]
    
    # cursor.execute("""
    #     SELECT * FROM profil_coefficients
    #     ;
    # """)
    # coeffs_df = cursor.fetchone()[0]

    # Fusion des tables
    df = temps_df.merge(coeffs_df, on="horodate", how="left")
    df = df.merge(holidays_df, left_on=df["horodate"].dt.date, right_on="date", how="left")

    # Calcul des champs temporels
    df["timestamp"] = pd.to_datetime(df["horodate"])
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
    schedule_interval=None,
    catchup=False,
    description="DAG to transform data from Postgres and load into DuckDB"
) as dag:

    transform_task = PythonOperator(
        task_id='transform_and_load_to_duckdb',
        python_callable=transform_and_load_to_duckdb
    )

