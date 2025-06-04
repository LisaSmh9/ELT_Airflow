from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import holidays
from vacances_scolaires_france import SchoolHolidayDates
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

def extract_and_load_vacances():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL_bronze")
    engine = create_engine(db_url)

    vacances_data = []
    school_holidays = SchoolHolidayDates()
    for annee in [2023, 2024, 2025]:
        fr_holidays = holidays.France(years=annee)
        vacances_test = school_holidays.holidays_for_year(annee)
        for date in pd.date_range(f"{annee}-01-01", f"{annee}-12-31"):
            date_obj = date.date()
            is_public_holiday = date_obj in fr_holidays
            public_holiday_name = fr_holidays.get(date_obj) if is_public_holiday else None
            nom_vacances = vacances_test.get(date_obj, {}).get("nom_vacances")
            zone_a = vacances_test.get(date_obj, {}).get("vacances_zone_a", False)
            zone_b = vacances_test.get(date_obj, {}).get("vacances_zone_b", False)
            zone_c = vacances_test.get(date_obj, {}).get("vacances_zone_c", False)
            vacances_data.append([
                date_obj, zone_a, zone_b, zone_c, nom_vacances, is_public_holiday, public_holiday_name
            ])
    df = pd.DataFrame(vacances_data, columns=[
        "date", "vacances_zone_a", "vacances_zone_b", "vacances_zone_c",
        "nom_vacances", "is_public_holiday", "public_holiday_name"
    ])
    df_filtered = df[(df["is_public_holiday"] == True) | (df["nom_vacances"].notnull())]
    df_filtered.to_sql("vacances_et_feries", con=engine, if_exists="replace", index=False)

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    'vacances_et_feries_dag',
    schedule_interval='0 1 1 * *',
    default_args=default_args,
    catchup=False
) as dag:
    extract_load_task = PythonOperator(
        task_id='extract_and_load_vacances',
        python_callable=extract_and_load_vacances
    )
dag.doc_md = __doc__