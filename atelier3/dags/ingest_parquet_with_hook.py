import logging
import os
import io
import pandas as pd

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.datasets import Dataset
from airflow.utils.email import send_email

# --- Définition du Dataset de sortie ---
COEFF_DATASET = Dataset("bronze/coefficients_electriques")

# Configuration
PARQUET_FOLDER = "/opt/airflow/local_parquet"
TABLE_NAME     = "coefficients_electriques"
KEY_COLS       = ["horodate", "sous_profil"]

# Logger
logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 4),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def _notify_success(context):
    ti     = context['task_instance']
    dag_id = ti.dag_id
    run_id = ti.run_id
    subject = f"[Airflow] DAG Success: {dag_id}"
    body = f"""
    DAG <b>{dag_id}</b> a réussi.<br>
    Run ID: {run_id}<br>
    Date: {context['ts']}<br>
    <a href="{ti.log_url}">Voir les logs</a>
    """
    # Remplacez par votre adresse Outlook
    send_email(to=["dag_airflow_elt_on_succes_or_fail@outlook.com"], subject=subject, html_content=body)

def _notify_failure(context):
    ti     = context['task_instance']
    dag_id = ti.dag_id
    run_id = ti.run_id
    subject = f"[Airflow] DAG Failure: {dag_id}"
    body = f"""
    ⚠️ DAG <b>{dag_id}</b> a échoué.<br>
    Run ID: {run_id}<br>
    Date: {context['ts']}<br>
    <a href="{ti.log_url}">Voir les logs</a>
    """
    send_email(to=["dag_airflow_elt_on_succes_or_fail@outlook.com"], subject=subject, html_content=body)

with DAG(
    dag_id="ingestion_parquet_postgres_multi",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
    tags=["elt", "parquet", "coeff"],
    description="Ingestion de fichiers Parquet avec UPSERT Postgres via hook.get_conn()",
    on_success_callback=_notify_success,
    on_failure_callback=_notify_failure,
) as dag:

    def ingest_multiple_parquets():
        logger.info("▶️ Démarrage de l’ingestion des fichiers Parquet")
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cur  = conn.cursor()

        # 1. Vérifier existence de la table
        cur.execute("SELECT to_regclass(%s);", (TABLE_NAME,))
        exists = cur.fetchone()[0] is not None
        logger.info("Table '%s' existe déjà ? %s", TABLE_NAME, exists)

        # 2. Si existante, charger les clés des 7 derniers jours
        recent_keys = set()
        if exists:
            sept = datetime.utcnow() - timedelta(days=7)
            logger.debug("→ clefs depuis %s", sept)
            cur.execute(
                f"SELECT horodate, sous_profil FROM {TABLE_NAME} WHERE horodate >= %s;",
                (sept,),
            )
            recent_keys = set(cur.fetchall())
            logger.info("Clés récentes chargées : %d", len(recent_keys))

        # 3. Parcours des fichiers
        for fname in sorted(os.listdir(PARQUET_FOLDER)):
            if not fname.endswith(".parquet"):
                continue
            done_flag = os.path.join(PARQUET_FOLDER, fname + ".done")
            if os.path.exists(done_flag):
                logger.debug("Fichier déjà traité, skip : %s", fname)
                continue

            logger.info("➡️ Traitement de %s", fname)
            df = pd.read_parquet(os.path.join(PARQUET_FOLDER, fname))
            df = df[["horodate","sous_profil",
                     "coefficient_ajuste",
                     "coefficient_dynamique",
                     "coefficient_prepare"]].copy()
            df["horodate"] = pd.to_datetime(df["horodate"])

            # Création initiale
            if not exists:
                logger.info("Création de la table %s", TABLE_NAME)
                cur.execute(f"""
                    CREATE TABLE {TABLE_NAME} (
                      horodate TIMESTAMP,
                      sous_profil TEXT,
                      coefficient_ajuste DOUBLE PRECISION,
                      coefficient_dynamique DOUBLE PRECISION,
                      coefficient_prepare DOUBLE PRECISION,
                      PRIMARY KEY (horodate, sous_profil)
                    );
                """)
                exists = True
                logger.debug("Table créée")

            # Séparation update vs insert
            df["key"]   = list(zip(df["horodate"], df["sous_profil"]))
            df_upd      = df[df["key"].isin(recent_keys)]
            df_ins      = df[~df["key"].isin(recent_keys)]
            logger.info("Lignes à mettre à jour : %d, à insérer : %d",
                        len(df_upd), len(df_ins))

            if df_upd.empty and df_ins.empty:
                logger.info("→ aucune donnée à traiter pour %s", fname)
                open(done_flag, "w").close()
                continue

            # Table temporaire
            tmp = TABLE_NAME + "_tmp"
            cur.execute(f"DROP TABLE IF EXISTS {tmp};")
            cur.execute(f"CREATE UNLOGGED TABLE {tmp} (LIKE {TABLE_NAME});")
            logger.debug("Table temporaire %s créée", tmp)

            to_tmp = pd.concat([df_upd, df_ins], ignore_index=True)
            buf    = io.StringIO()
            to_tmp[["horodate","sous_profil",
                    "coefficient_ajuste",
                    "coefficient_dynamique",
                    "coefficient_prepare"]].to_csv(buf,
                                                   index=False,
                                                   header=False)
            buf.seek(0)
            cur.copy_expert(f"COPY {tmp} FROM STDIN WITH CSV", buf)
            logger.debug("Chargé dans %s", tmp)

            # UPSERT
            set_clause = ", ".join(f"{c}=EXCLUDED.{c}" for c in [
                "coefficient_ajuste",
                "coefficient_dynamique",
                "coefficient_prepare"
            ])
            cols = ["horodate","sous_profil"] + [
                "coefficient_ajuste",
                "coefficient_dynamique",
                "coefficient_prepare"
            ]
            cur.execute(f"""
                INSERT INTO {TABLE_NAME} ({','.join(cols)})
                SELECT {','.join(cols)} FROM {tmp}
                ON CONFLICT (horodate, sous_profil)
                  DO UPDATE SET {set_clause};
                DROP TABLE {tmp};
            """)
            conn.commit()
            logger.info("✅ %s ingéré et upsert effectué", fname)

            # Marquer traité
            open(done_flag, "w").close()
            logger.debug("Marqué traité : %s", done_flag)

        cur.close()
        conn.close()
        logger.info("✔️ Ingestion terminée")

    ingest_task = PythonOperator(
        task_id="ingest_all_new_parquets",
        python_callable=ingest_multiple_parquets,
        outlets=[COEFF_DATASET],
    )
