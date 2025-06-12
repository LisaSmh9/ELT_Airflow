import pandas as pd
from datetime import datetime
import pytest
import os
import sys

# Import des fonctions à tester
dags_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dags'))
sys.path.append(dags_path)
from create_gold_table import transform_data


@pytest.fixture
def sample_dataframes():
    """
    Crée et retourne un jeu de DataFrames de test pour simuler les entrées.
    Ceci est une "fixture" pytest : elle prépare des données réutilisables.
    """
    holidays_data = {
        'date': [datetime(2023, 5, 1).date(), datetime(2023, 5, 2).date()],
        'vacances_zone_a': [True, False],
        'vacances_zone_b': [True, False],
        'vacances_zone_c': [False, False],
        'is_public_holiday': [True, False],
    }
    holidays_df = pd.DataFrame(holidays_data)

    # Note : On simule un timestamp avec un fuseau horaire, comme dans la vraie base de données.
    temps_data = {
        'horodate': [pd.Timestamp('2023-05-01 10:00:00', tz='Europe/Paris')],
        'temperature_realisee_lissee_degc': [15.5],
        'temperature_normale_lissee_degc': [14.0],
    }
    temps_df = pd.DataFrame(temps_data)
    
    coeffs_data = {
        'horodate': [pd.Timestamp('2023-05-01 10:00:00', tz='Europe/Paris')],
        'coefficient_prepare': [1.15],
        'sous_profil': ['RES-BTA-1'],
    }
    coeffs_df = pd.DataFrame(coeffs_data)
    
    return holidays_df, temps_df, coeffs_df


def test_transform_data_logic(sample_dataframes):
    """
    Teste la fonction de transformation de bout en bout avec des données simulées.
    """
    # 1. Préparation (Arrange)
    # On récupère les données factices préparées par la fixture.
    holidays_df, temps_df, coeffs_df = sample_dataframes
    
    # 2. Action (Act)
    # On appelle la fonction à tester avec nos données.
    result_df = transform_data(holidays_df, temps_df, coeffs_df)
    
    # 3. Assertions (Assert)
    # On vérifie que le résultat est celui attendu.
    
    # -- Vérification de la structure globale --
    assert isinstance(result_df, pd.DataFrame)
    assert not result_df.empty
    assert result_df.shape[0] == 1 # On s'attend à une seule ligne de résultat

    # -- Vérification du renommage des colonnes --
    expected_columns = ['timestamp', 'trl', 'tnl', 'cp', 'sous_profil', 'day_of_week', 
                        'day_of_year', 'half_hour', 'fr_holiday', 'is_public_holiday']
    assert all(col in result_df.columns for col in expected_columns)
    
    # On stocke la seule ligne de résultat pour faciliter les vérifications
    row = result_df.iloc[0]

    # -- Vérification des valeurs et des fusions --
    assert row['trl'] == 15.5
    assert row['cp'] == 1.15
    assert row['sous_profil'] == 'RES-BTA-1'
    
    # -- Vérification de la logique métier --
    
    # La fusion avec les jours fériés a-t-elle fonctionné ? (1er mai)
    assert row['is_public_holiday'] == True
    
    # Le calcul de 'fr_holiday' est-il correct ? (110 = zone A et B en vacances, C ne l'est pas)
    assert row['fr_holiday'] == '110'
    
    # Les colonnes de date/heure sont-elles correctes ? (le 1er mai 2023 était un lundi)
    assert row['day_of_week'] == 0 # Lundi = 0
    
    assert row['half_hour'] == 16 # 08h00 UTC -> 8 * 2 + 0 // 30 = 16
    
    # -- Vérification des types de données finaux --
    assert pd.api.types.is_datetime64_any_dtype(result_df['timestamp'])
    assert pd.api.types.is_bool_dtype(result_df['is_public_holiday'])
    assert pd.api.types.is_numeric_dtype(result_df['trl'])
