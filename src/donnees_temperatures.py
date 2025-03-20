# Imports
import requests
import pandas as pd
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
import os

# Chargement des variables d'environnement
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Connexion à la base de données
engine = create_engine(DATABASE_URL)

def bdd_connect(table_name):
    """
    Charge une table SQL dans un DataFrame pandas.
    """
    return pd.read_sql(f"SELECT * FROM {table_name}", con=engine)


def fetch_temperatures(beginning_date, end_date):
    """
    Récupère toutes les températures via pagination API entre deux dates.
    """
    base_url = "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/donnees-de-temperature-et-de-pseudo-rayonnement/records"
    all_data = []
    offset = 0
    limit = 100

    while True:
        params = {
            "where": f"horodate >= '{beginning_date}' AND horodate < '{end_date}'",
            "timezone": "Europe/Paris",
            "limit": limit,
            "offset": offset
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


def update_temperature_table(engine):
    """
    Fonction principale qui crée ou met à jour la table 'temperatures' dans la base PostgreSQL.
    - Récupère toutes les données de l'API jour par jour
    - Ajoute un index sur la colonne horodate
    """
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    if 'temperatures' not in tables:
        df = pd.DataFrame()
        beginning_date = date(2023, 1, 1)

        while beginning_date <= datetime.now().date():
            end_date = beginning_date + timedelta(days=1)
            df_inter = fetch_temperatures(beginning_date, end_date)
            df = pd.concat([df, df_inter], ignore_index=True)
            beginning_date = end_date

        if not df.empty:
            df['horodate'] = pd.to_datetime(df['horodate'])
            df.to_sql('temperatures', con=engine, if_exists='replace', index=False)

            with engine.connect() as conn:
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_date ON public.temperatures(horodate);
                """))

            engine.dispose()

    else:
        # Mise à jour depuis la dernière date existante
        df_bdd = bdd_connect('temperatures')
        last_date_str = df_bdd['annee_mois_jour'].max()
        beginning_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()

        df = pd.DataFrame()
        while beginning_date <= datetime.now().date():
            end_date = beginning_date + timedelta(days=1)
            df_inter = fetch_temperatures(beginning_date, end_date)
            df = pd.concat([df, df_inter], ignore_index=True)
            beginning_date = end_date

        if not df.empty:
            df['horodate'] = pd.to_datetime(df['horodate'])
            df.to_sql('temperatures', con=engine, if_exists='append', index=False)


# if __name__ == "__main__":
#     update_temperature_table(engine)
#     print("Script terminé avec succès.")
