# Utilise l’image officielle d’Airflow 3.0.0 comme base
FROM apache/airflow:3.0.0

# Installe les bibliothèques Python nécessaires
USER root

# Repasse à l'utilisateur airflow pour exécuter pip
USER airflow

# Installe les librairies Python spécifiques au projet
RUN pip install --no-cache-dir \
    vacances_scolaires_france \
    holidays
