# -- Import librairie --
import pytest
import pandas as pd
from unittest.mock import MagicMock
import os 
import sys
from datetime import datetime
import pytest
import requests

# Import des fonctions à tester
dags_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dags'))
sys.path.append(dags_path)
from elt_temperature_pipeline import fetch_temperatures, update_temperature_table


# =============================================================================
# =============     Test en cas de succès de l'API     ========================
# =============================================================================
def test_fetch_temperatures_success(mocker):
    """
    Teste le cas où l'API Enedis répond correctement avec des données.
    """
    # Simulation d'une réponse API avec des données
    fake_api_response = {
        "total_count": 2,
        "results": [
            {'horodate': '2024-01-01T10:00:00+01:00', 'temperature_realisee_lissee_degc': 12.5},
            {'horodate': '2024-01-01T10:30:00+01:00', 'temperature_realisee_lissee_degc': 13.0},
        ]
    }
    # On mocke l'appel `requests.get` dans le fichier du DAG
    mock_get = mocker.patch('elt_temperature_pipeline.requests.get')
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = fake_api_response

    # On exécute la fonction à tester
    start_date = "2024-01-01"
    end_date = "2024-01-02"
    result_df = fetch_temperatures(start_date, end_date)

    # Vérification
    mock_get.assert_called_once()
    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == 2


# =============================================================================
# =============     Test en cas d'echec de l'API     ==========================
# =============================================================================

def test_fetch_temperatures_api_error(mocker):
    """
    Teste le cas où l'API Enedis répond avec une erreur (ex: 500).
    La fonction doit lever une exception.
    """
    print("Lancement du test pour fetch_temperatures en cas d'erreur API...")

    # On simule une réponse d'erreur de l'API

    # On intercepte à nouveau l'appel à `requests.get`
    mock_get = mocker.patch('dags.elt_temperature_pipeline.requests.get')
    
    # On crée une fausse réponse qui simule une erreur serveur
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.reason = "Internal Server Error"
    
    # on dit au mock que lorsqu'on appelle la méthode .raise_for_status(), il doit lever une HTTPError,
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError
    
    # On configure l'intercepteur pour qu'il retourne notre fausse réponse d'erreur
    mock_get.return_value = mock_response

    # On vérifie que notre fonction lève bien une exception
    
    with pytest.raises(requests.exceptions.HTTPError):
        start_date = "2024-01-01"
        end_date = "2024-01-02"
        fetch_temperatures(start_date, end_date)

    # On peut aussi vérifier que la fonction a bien tenté d'appeler l'API
    mock_get.assert_called_once()
    
    print("Test d'erreur réussi ! L'exception a bien été levée.")