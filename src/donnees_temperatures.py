import requests
import pandas as pd
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
import os

# Chargement des variables d'environnement
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL_BRONZE")
engine = create_engine(DATABASE_URL)

def bdd_connect(table_name):
    return pd.read_sql(f"SELECT * FROM {table_name}", con=engine)

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

def check_and_create_index(engine, index_name, table_name, column_name):
    """Vérifie si l'index existe, sinon le crée"""
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT 1 FROM pg_indexes 
            WHERE tablename = '{table_name}' 
            AND indexname = '{index_name}';
        """)).fetchone()

        if result:
            pass
        else:
            conn.execute(text(f"""
                CREATE INDEX {index_name} 
                ON public.{table_name}({column_name});
            """))

def update_temperature_table(engine):
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
            df['horodate'] = pd.to_datetime(df['horodate'], utc=True).dt.tz_convert('Europe/Paris')
            df.head(0).to_sql('temperatures', con=engine, if_exists='replace', index=False)
            df.to_sql('temperatures', con=engine, if_exists='append', index=False)
    else:
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
            df['horodate'] = pd.to_datetime(df['horodate'], utc=True).dt.tz_convert('Europe/Paris')
            df.to_sql('temperatures', con=engine, if_exists='append', index=False)

    # Recréation de l'index si jamais il saute
    check_and_create_index(engine, index_name="idx_date", table_name="temperatures", column_name="horodate")

if __name__ == "__main__":
    update_temperature_table(engine)
    engine.dispose()
    print("Script terminé avec succès")
