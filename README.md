# Projet ELT avec Apache Airflow – Prédiction de la consommation énergétique

## 🎯 Objectif du projet
Ce projet a pour objectif de mettre en place une pipeline ELT (Extract → Load → Transform)  orchestrée avec Apache Airflow, pour collecter, stocker et transformer des données nécessaires à la prédiction de la consommation énergétique (coefficient des profils).
Il permet de collecter automatiquement les données suivantes :
- Les températures normales et réalisées (depuis une API publique)
- Les jours fériés et vacances scolaires en France
- Les coefficients de profils (depuis un fichier Parquet)
Ces données sont chargées dans **PostgreSQL (bronze)**, puis transformées et stockées dans **DuckDB (gold)**.

## 🚀 Lancement du projet
```bash
git clone https://github.com/LisaSmh9/ELT_Airflow.git
cd atelier3
```
## 🧩 Structure du projet
```bash
atelier3/
├── dags/                      # Contient les DAGs Airflow définissant les étapes du pipeline
│   ├── dag_vacancesscolaires.py     # Extraction des vacances scolaires et jours fériés
│   ├── elt_temperature_pipeline.py  # Pipeline d’extraction des températures
│   ├── ingest_parquet_with_hook.py  # Ingestion des coefficients de profils (parquet)
│   ├── create_gold_table.py         # Création de la table finale dans DuckDB (gold)
│   ├── dag_full_refresh.py          # Tâche de suppression des tables (full refresh)
│   └── main_dag.py                  
│
├── tests/                     # Tests unitaires pour valider chaque composant du pipeline
│   ├── test_dag_vacancesscolaires.py
│   ├── test_elt_temperature_pipeline.py
│   ├── test_ingest_parquet.py
│   └── test_gold_table.py
│
├── CDP/                       # Contient le fichier coefficients-des-profils.parquet
├── docker/                    # Configuration Docker 
├── logs/                      # Stockage des logs Airflow

├── Dockerfile                 # Image Docker personnalisée pour Airflow
├── docker-compose.yml         # Définit les services : Airflow, PostgreSQL, DuckDB...
requirements.txt               # Dépendances Python
README.md                      # Documentation du projet 
```

## 🐳 Installation avec Docker


### 🛠️ 1. Construire l’image Docker
Exécutez la commande suivante pour construire l’image Docker :
```bash
docker build -t airflow_custom .
```
Cette commande utilise le **Dockerfile** présent dans le dossier pour créer une image Docker nommée **airflow_custom**.

#### Vérifier la création de l’image :
Une fois la construction terminée, listez les images Docker disponibles :
```bash
docker images
```
Vous devriez voir une image nommée airflow_custom dans la liste.

### 🚀 2. Démarrer Apache Airflow avec Docker Compose
Lancez tous les services avec :
```bash
docker-compose up -d
```
Cette commande démarre Apache Airflow ainsi que ses dépendances (PostgreSQL, DuckDB...).

### 🌐 3. Accéder à l'interface Airflow
- Ouvrez un navigateur et allez sur http://localhost:8080.
- Connectez-vous avec :
     - **Utilisateur** : airflow
     - **Mot de passe** : airflow

## ⛏️ Description des DAGs
Le projet est composé de plusieurs **DAGs Apache Airflow**, chacun orchestrant une étape précise du pipeline ELT :

| DAG | Description |
|-----|-------------|
| `dag_vacancesscolaires.py` | Extrait les jours fériés (librairie `holidays`) et vacances scolaires (librairie `vacances_scolaires_france`) pour les années 2023 à 2025, puis les stocke dans la base PostgreSQL. |
| `elt_temperature_pipeline.py` | Récupère les températures réalisées et normales depuis l’API d’Enedis, jour par jour et remplit une table PostgreSQL `temperatures`. |
| `ingest_parquet_with_hook.py` | Ingère les coefficients de profils depuis un fichier `.parquet` (présent dans le dossier `CDP/`), en effectuant un **UPSERT** dans PostgreSQL sur les clés `(horodate, sous_profil)`. |
| `create_gold_table.py` | Fusionne les trois sources (vacances, températures, coefficients) pour créer une table finale propre dans **DuckDB** (`data_model_inputs`) avec toutes les transformations temporelles nécessaires. |
| `dag_full_refresh.py` | Supprime toutes les tables de la base (bronze et gold) si confirmé manuellement, puis enchaîne automatiquement l'exécution de tous les DAGs métiers. |

## 🧪 Tests
### Structure des tests

| Test | Description |
|-----|-------------|
| `test_dag_vacancesscolaires.py` | Vérifie que le DAG des vacances s’importe correctement et que ses tâches sont bien définies. |
| `test_elt_temperature_pipeline.py` | Mocke les appels à l’API Enedis pour tester la récupération de températures (succès + échec HTTP). |
| `test_gold_table.py` | Valide la logique de transformation finale vers la table data_model_inputs de DuckDB. |
| `test_ingest_parquet.py` | Simule l’ingestion de fichiers .parquet, vérifie le comportement d’idempotence et la gestion des .done. |

### Lancer les tests
```bash
pytest tests/
```

## 🧯 Monitoring

Le projet intègre un mécanisme de monitoring via des callbacks définis dans dags/utils/callbacks_modules.py.
Ces fonctions permettent de notifier automatiquement l’équipe en cas de succès ou d’échec des tâches critiques dans les DAGs.

🔔 Notifications

**notify_success(context) :** appelée automatiquement lorsqu’une tâche se termine avec succès.

**notify_failure(context) :** appelée lorsqu’une tâche échoue.

## 🔁 Mode Full Refresh

Le DAG **dag_full_refresh** permet un nettoyage total des données et une relance séquentielle de tous les DAGs du projet.

### ✅ Fonctionnement

Suppression des tables : temperatures, profil_coefficients, holidays, data_model_inputs

Recréation des données depuis les sources (API, Librairies, Fichier)

Rechargement complet de la table finale dans DuckDB
```bash
⚠️ ATTENTION : Opération irréversible
```


## 🧰 Stack technique

**Orchestration :** Apache Airflow (v2.9.1)

**Base de données :** PostgreSQL (bronze)

**Data Warehouse :** DuckDB (gold)

**Conteneurisation :** Docker, Docker Compose

**Langage :** Python

**Tests :** Pytest

**Librairies data :** pandas, sqlalchemy, holidays, vacances_scolaires_france


