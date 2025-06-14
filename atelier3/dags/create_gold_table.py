# --- Import librairies et modules ---
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from utils.callbacks_modules import notify_failure, notify_success
from airflow.datasets import Dataset

# Chargement des variables d'environnement
load_dotenv() 

# Création des datasets pour le déclenchement
temperature_table_dataset = Dataset("table_temperatures")
coefficients_table_dataset = Dataset("table_profil_coefficients")

# Constante pour la date de début
begining_date_str = "2023-01-01"

# --- Logique de la tâche ---

def transform_data(holidays_df: pd.DataFrame, temps_df: pd.DataFrame, coeffs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Prend des DataFrames en entrée, retourne un DataFrame transformé.
    Cette fonction est testable unitairement car elle ne dépend d'aucune connexion externe.
    """
    # Transformation des horodate sur le même fuseau
    temps_df['horodate'] = pd.to_datetime(temps_df['horodate']).dt.tz_convert('UTC')
    coeffs_df['horodate'] = pd.to_datetime(coeffs_df['horodate']).dt.tz_localize('Europe/Paris').dt.tz_convert('UTC')

    # Fusion des tables
    df = coeffs_df.merge(temps_df, on="horodate", how="left")
    
    # Création colonne date_only pour faciliter la jointure
    df["date_only"] = df["horodate"].dt.date
    # S'assurer que la colonne 'date' dans holidays_df est au bon format
    holidays_df['date'] = pd.to_datetime(holidays_df['date']).dt.date
    df = df.merge(holidays_df, left_on="date_only", right_on="date", how="left")
    df.drop(columns=["date_only", "date"], inplace=True) # Supprimer les colonnes de jointure

    # Calcul des champs temporels demandés
    df['timestamp'] = pd.to_datetime(df['horodate'])
    df["day_of_week"] = df["timestamp"].dt.weekday
    df["day_of_year"] = df["timestamp"].dt.dayofyear
    df["half_hour"] = df["timestamp"].dt.hour * 2 + df["timestamp"].dt.minute // 30

    # fr_holiday 
    df["fr_holiday"] = df[["vacances_zone_a", "vacances_zone_b", "vacances_zone_c"]].fillna(0).astype(int).astype(str).agg("".join, axis=1)

    # is_public_holiday
    df["is_public_holiday"] = df["is_public_holiday"].fillna(False).astype(bool)

    # Sélection des colonnes finales et renommage
    final_columns = ['timestamp', 'temperature_realisee_lissee_degc', 'temperature_normale_lissee_degc', 'coefficient_prepare', 
                     "sous_profil","day_of_week", "day_of_year", "half_hour","fr_holiday", "is_public_holiday"]
    df_final = df[final_columns]
    
    df_final = df_final.rename(columns={
        'temperature_realisee_lissee_degc': 'trl', 
        'temperature_normale_lissee_degc': 'tnl', 
        'coefficient_prepare': 'cp'
    })

    return df_final

def extract_transform_and_load():
    """
    Fonction orchestratrice appelée par l'Operator.
    Gère l'extraction, appelle la logique de transformation, et charge les données (IO).
    """
    # EXTRACTION 
    print("Connecting to PostgreSQL to extract data...")
    hook = PostgresHook(postgres_conn_id="postgres_bronze")
    holidays_df = hook.get_pandas_df(f"SELECT * FROM holidays WHERE date >= '{begining_date_str}'")
    temps_df = hook.get_pandas_df(f"SELECT * FROM temperatures WHERE horodate >= '{begining_date_str}'")
    coeffs_df = hook.get_pandas_df(f"SELECT * FROM profil_coefficients WHERE horodate >= '{begining_date_str}'")
    print("Extraction complete.")

    # TRANSFORMATION 
    print("Transforming data...")
    df_transformed = transform_data(holidays_df, temps_df, coeffs_df)
    print("Transformation complete.")

    # --- Tests de qualité ---
    if df_transformed['timestamp'].isnull().any():
        raise ValueError("Validation Error: La colonne 'timestamp' ne doit pas contenir de valeurs nulles.")

    #  CHARGEMENT
    print("Connecting to DuckDB to load data...")
    duckdb_conn_str = os.getenv("DATABASE_URL_gold")
    if not duckdb_conn_str:
        raise ValueError("La variable d'environnement DATABASE_URL_gold n'est pas définie.")
    
    engine = create_engine(duckdb_conn_str)
    
    df_transformed.to_sql("data_model_inputs", con=engine, if_exists="replace", index=False)
    print(f"{len(df_transformed)} rows loaded successfully into DuckDB table 'data_model_inputs'.")

# Arguments par défaut du DAG
default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# --- Définition du DAG ---

with DAG(
    'transform_postgres_to_duckdb',
    default_args=default_args,
    schedule=[temperature_table_dataset, coefficients_table_dataset],
    catchup=False,
    description="DAG to transform data from Postgres and load into DuckDB",
) as dag:
    transform_task = PythonOperator(
        task_id='transform_and_load_to_duckdb',
        python_callable=extract_transform_and_load, 
        on_success_callback=notify_success,
        on_failure_callback=notify_failure,
    )
