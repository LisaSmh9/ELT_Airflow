# 🚀 Installation et exécution avec Docker

## 📥 1. Télécharger et extraire le dossier atelier 
- Téléchargez l'archive `atelier.zip` du projet et **décompressez-la**.
- Accédez au dossier **atelier** dans le terminal :
```bash
  cd atelier
```

## 🛠️ 2. Construire l’image Docker
Exécutez la commande suivante pour construire l’image Docker :
```bash
docker build -t airflow_custom .
```
Cette commande utilise le **Dockerfile** présent dans le dossier pour créer une image Docker nommée **airflow_custom**.

### Vérifier la création de l’image :
Une fois la construction terminée, listez les images Docker disponibles :
```bash
docker images
```
Vous devriez voir une image nommée airflow_custom dans la liste.

## 🚀 3. Démarrer Apache Airflow avec Docker Compose
Lancez tous les services avec :
```bash
docker-compose up -d
```
Cette commande démarre Apache Airflow ainsi que ses dépendances (PostgreSQL, DuckDB...).

## 🌐 4. Accéder à l'interface Airflow
- Ouvrez un navigateur et allez sur http://localhost:8080.
- Connectez-vous avec :
     - **Utilisateur** : airflow
     - **Mot de passe** : airflow
 
## 🔄 5. Mettre à jour vers Apache Airflow 2.9 (aka "Airflow 3")
- Assurez-vous que votre Dockerfile contient : FROM apache/airflow:2.9.1
- Reconstruire L'environnement avec les étapes suivantes :
```bash
docker-compose down
docker-compose build
docker-compose up airflow-init
docker-compose up
```

## ⚠️ Problèmes fréquents
- Conflit de conteneur déjà existant (ex : Error: container name "duckdb_gold" is already in use) :
```bash
docker rm -f duckdb_gold
```

## 🧪 Vérification de la version d’Airflow
```bash
docker-compose run --rm airflow-webserver airflow version
```







