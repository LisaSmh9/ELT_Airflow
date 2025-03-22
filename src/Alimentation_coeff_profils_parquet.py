from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# Fichier Parquet
FILE_PATH = "../CDP/coefficients-des-profils.parquet"

# Nom de la table PostgreSQL
TABLE_NAME = "coefficients_profils"

# Fonction pour créer la table si elle n'existe pas et insérer les données
def load_parquet_to_postgres():
    try:
        hook = PostgresHook(postgres_conn_id="POSTGRES_DEFAULT")
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Création de la table si elle n'existe pas
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            horodate TIMESTAMP PRIMARY KEY,
            sous_profil TEXT,
            categorie TEXT,
            coefficient_prepare FLOAT,
            coefficient_ajuste FLOAT,
            coefficient_dynamique FLOAT,
            indic_dispers_poids_dyn FLOAT,
            indic_precision_dyn FLOAT
        );
        """
        cursor.execute(create_table_sql)
        conn.commit()
        print("✅ Table vérifiée/créée.")

        # Vider la table
        cursor.execute(f"TRUNCATE TABLE {TABLE_NAME};")
        conn.commit()
        print("✅ Table vidée.")
        
        # Lire les données du fichier Parquet
        df = pd.read_parquet(FILE_PATH)

        # Insérer les données
        for _, row in df.iterrows():
            insert_sql = f"""
            INSERT INTO {TABLE_NAME} (
                horodate, sous_profil, categorie, coefficient_prepare,
                coefficient_ajuste, coefficient_dynamique, 
                indic_dispers_poids_dyn, indic_precision_dyn
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (horodate) DO NOTHING;
            """
            cursor.execute(insert_sql, tuple(row))
        
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Données insérées avec succès.")

    except Exception as e:
        print(f"❌ Erreur : {e}")
        raise e

# Définition du DAG
default_args = {
    "start_date": datetime(2024, 3, 21),
}

with DAG("load_parquet_to_postgres", schedule=None, default_args=default_args, catchup=False) as dag:
    load_data_task = PythonOperator(
        task_id="load_parquet_data",
        python_callable=load_parquet_to_postgres
    )
