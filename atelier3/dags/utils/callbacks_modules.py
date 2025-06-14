from dotenv import load_dotenv
from airflow.utils.email import send_email
from zoneinfo import ZoneInfo

load_dotenv() 

def notify_success(context):
    exec_date = context['execution_date']
    # Convertir en timezone Europe/Paris
    exec_date_paris = exec_date.astimezone(ZoneInfo("Europe/Paris"))
    exec_date_str = exec_date_paris.strftime("%Y-%m-%d %H:%M:%S %Z")
    subject =  "Exécution réussie !"
    body = f"Le DAG {context['dag'].dag_id} a terminé avec succès à {exec_date_str}."
    send_email(to="emmanuelle.le-gal@supdevinci-edu.fr", subject=subject, html_content=body)

def notify_failure(context):
    exec_date = context['execution_date']
    exec_date_paris = exec_date.astimezone(ZoneInfo("Europe/Paris"))
    exec_date_str = exec_date_paris.strftime("%Y-%m-%d %H:%M:%S %Z")
    subject = f"L'exécution à échoué !"
    body = f"""
    Le DAG {context['dag'].dag_id} a échoué à {exec_date_str}
    Task : {context['task_instance'].task_id}
    Erreur : {context['exception']}
    """
    send_email(to="emmanuelle.le-gal@supdevinci-edu.fr", subject=subject, html_content=body)