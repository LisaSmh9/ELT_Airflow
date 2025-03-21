import pandas as pd
import holidays
from vacances_scolaires_france import SchoolHolidayDates
from datetime import datetime

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

    # 🔍 Filtrer uniquement les jours fériés ou jours de vacances
    df_filtres = df_vacances[
        (df_vacances["is_public_holiday"] == True) |
        (df_vacances["nom_vacances"].notnull())
    ]

    return df_filtres


df_vacances_filtres = collecter_vacances_et_feries([2023, 2024, 2025])
df_vacances_filtres.head(60)