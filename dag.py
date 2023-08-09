from airflow.models import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime

from dataset.extract import get_zip, unzip_dataset

# Create a DAG
dag = DAG(
    dag_id='example_papermill_operator',
    start_date=datetime(2023, 8, 5),
    schedule_interval='@weekly',
)

transform_annual_value_task = PapermillOperator(
    task_id="run_transform_annual_value_notebook",
    input_nb="transform_annual_value.ipynb",
    output_nb="output/transform_annual_value.ipynb",
    # dag=dag,
)

transform_consumer_price_indices_task = PapermillOperator(
    task_id="run_transform_consumer_price_indices_notebook",
    input_nb="transform_consumer_price_indices.ipynb",
    output_nb="output/transform_consumer_price_indices.ipynb",
    # dag=dag,
)

transform_country_task = PapermillOperator(
    task_id="run_transform_country_notebook",
    input_nb="transform_country.ipynb",
    output_nb="output/transform_country.ipynb",
    # dag=dag,
)

transform_deflator_task = PapermillOperator(
    task_id="run_transform_deflator_notebook",
    input_nb="transform_deflator.ipynb",
    output_nb="output/transform_deflator.ipynb",
    # dag=dag,
)

transform_item_prod_task = PapermillOperator(
    task_id="run_transform_item_prod_notebook",
    input_nb="transform_item_prod.ipynb",
    output_nb="output/transform_item_prod.ipynb",
    # dag=dag,
)

transform_price_task = PapermillOperator(
    task_id="run_transform_price_notebook",
    input_nb="transform_price.ipynb",
    output_nb="output/transform_price.ipynb",
    # dag=dag,
)

transform_trade_crops_livestock_task = PapermillOperator(
    task_id="run_transform_trade_crops_livestock_notebook",
    input_nb="transform_trade_crops_livestock.ipynb",
    output_nb="output/transform_trade_crops_livestock.ipynb",
    # dag=dag,
)

transform_trade_detailed_trade_matrix_task = PapermillOperator(
    task_id="run_transform_trade_detailed_trade_matrix_notebook",
    input_nb="transform_trade_detailed_trade_matrix.ipynb",
    # dag=dag,
    output_nb="output/transform_trade_detailed_trade_matrix.ipynb",
)

get_zip_task = PythonOperator(
    task_id="get_zip",
    python_callable=get_zip,
    op_args=[],
    dag=dag,
)

unzip_dataset_task = PythonOperator(
    task_id="unzip_dataset",
    python_callable=unzip_dataset,
    op_args=[],
    # dag=dag,
)

get_zip_task
# >> unzip_dataset_task >> [transform_annual_value_task, transform_consumer_price_indices_task, transform_country_task, transform_deflator_task, transform_item_prod_task, transform_price_task, transform_trade_crops_livestock_task, transform_trade_detailed_trade_matrix_task]

dag.run()
