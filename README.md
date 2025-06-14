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



