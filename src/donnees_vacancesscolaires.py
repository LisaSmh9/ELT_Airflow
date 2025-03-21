import pandas as pd
import holidays
from vacances_scolaires_france import SchoolHolidayDates
from datetime import datetime
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
import os

# Chargement des variables d'environnement
load_dotenv()
DATABASE_URL_bronze = os.getenv("DATABASE_URL_bronze")

# Connexion à la base de données
engine = create_engine(DATABASE_URL_bronze)

def bdd_connect(table_name):
    """
    Charge une table SQL dans un DataFrame pandas.
    """
    return pd.read_sql(f"SELECT * FROM {table_name}", con=engine)

def collecter_vacances_et_feries(annees=[2023, 2024, 2025]):
    vacances_data = []
    school_holidays = SchoolHolidayDates()

    for annee in annees:
        fr_holidays = holidays.France(years=annee)
        vacances_test = school_holidays.holidays_for_year(annee)
        start_date = datetime(annee, 1, 1)
        end_date = datetime(annee, 12, 31)
        date_range = pd.date_range(start=start_date, end=end_date)

        for date in date_range:
            date_obj = date.to_pydatetime().date()

            is_public_holiday = date_obj in fr_holidays
            public_holiday_name = fr_holidays.get(date_obj) if is_public_holiday else None

            nom_vacances = None
            if date_obj in vacances_test:
                nom_vacances = vacances_test[date_obj].get("nom_vacances", None)
                if isinstance(nom_vacances, str):
                    try:
                        nom_vacances = nom_vacances.encode('latin1').decode('utf-8')
                    except:
                        pass  # en cas d'erreur de décodage

            zone_a = vacances_test.get(date_obj, {}).get("vacances_zone_a", False)
            zone_b = vacances_test.get(date_obj, {}).get("vacances_zone_b", False)
            zone_c = vacances_test.get(date_obj, {}).get("vacances_zone_c", False)

            vacances_data.append([
                date_obj, zone_a, zone_b, zone_c, nom_vacances, is_public_holiday, public_holiday_name
            ])

    df_vacances = pd.DataFrame(vacances_data, columns=[
        "date", "vacances_zone_a", "vacances_zone_b", "vacances_zone_c",
        "nom_vacances", "is_public_holiday", "public_holiday_name"
    ])

    # Filtrer uniquement les jours fériés ou jours de vacances
    df_filtres = df_vacances[
        (df_vacances["is_public_holiday"] == True) |
        (df_vacances["nom_vacances"].notnull())
    ]

    return df_filtres

def update_vacances_table(engine):
    df = collecter_vacances_et_feries([2023, 2024, 2025])
    df.to_sql('vacancesscolaires_joursferies', con=engine, if_exists='replace', index=False)

    with engine.connect() as conn:
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_vacances_date 
            ON public.vacancesscolaires_joursferies(date);
        """))
    print("✅ Table 'vacancesscolaires_joursferies' insérée/mise à jour.")

if __name__ == "__main__":
    update_vacances_table(engine)
