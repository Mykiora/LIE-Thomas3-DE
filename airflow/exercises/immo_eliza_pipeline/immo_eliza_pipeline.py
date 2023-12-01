from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator


# ------ 1 : Definition of all the required functions ------
# def scrape_apartments():


def store_data(data):
    print(f"{data} data saved in the database !")


def analysis_preprocessing():
    print(f"Data ready for analysis !")


def training_preprocessing():
    print(f"Data ready for training !")


def make_dashboard():
    print("Yaaay, dashboards !")


def train_model():
    print("Model is trained !!")


def save_model():
    print("Model saved !!")


# ----------------------------------------------------------

# ------ 2 : Definition of the DAG ------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 11, 30),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "immo_eliza_pipeline",
    default_args=default_args,
    description="The pipeline asked by the boss :)",
    schedule_interval="0 22 * * *",
)
# ---------------------------------------

# ------ 3 : Definition of the operators ------
# The folder inside which your project pipeline's scripts are stored (absolute path)
file_path = "/mnt/d/airflow/scripts/scraper"

scrape_apartments = BashOperator(
    task_id="scrape_apartments",
    bash_command=f"python3 {file_path}/main_apartments.py",
    dag=dag,
)

scrape_houses = BashOperator(
    task_id="scrape_houses",
    bash_command=f"python3 {file_path}/main_houses.py",
    dag=dag,
)

store_raw_data = PythonOperator(
    task_id="store_raw_data",
    provide_context=True,
    python_callable=store_data,
    op_kwargs={"data": "raw"},
    dag=dag,
)

data_cleaning_analysis = PythonOperator(
    task_id="data_cleaning_analysis",
    provide_context=True,
    python_callable=analysis_preprocessing,
    dag=dag,
)

data_cleaning_training = PythonOperator(
    task_id="data_cleaning_training",
    provide_context=True,
    python_callable=training_preprocessing,
    dag=dag,
)

store_analysis_data = PythonOperator(
    task_id="store_analysis_data",
    provide_context=True,
    python_callable=store_data,
    op_kwargs={"data": "analysis"},
    dag=dag,
)

store_training_data = PythonOperator(
    task_id="store_training_data",
    provide_context=True,
    python_callable=store_data,
    op_kwargs={"data": "training"},
    dag=dag,
)

make_dashboard = PythonOperator(
    task_id="make_dashboard",
    provide_context=True,
    python_callable=make_dashboard,
    dag=dag,
)

train_model = PythonOperator(
    task_id="train_model",
    provide_context=True,
    python_callable=train_model,
    dag=dag,
)

store_model = PythonOperator(
    task_id="store_model",
    provide_context=True,
    python_callable=save_model,
    dag=dag,
)
# ---------------------------------------------

# ------ 4 : Order of execution ------
scrape_apartments >> store_raw_data
scrape_houses >> store_raw_data
store_raw_data >> data_cleaning_analysis
store_raw_data >> data_cleaning_training
data_cleaning_analysis >> store_analysis_data
store_analysis_data >> make_dashboard
data_cleaning_training >> store_training_data
store_training_data >> train_model
train_model >> store_model
# -------------------------------------
