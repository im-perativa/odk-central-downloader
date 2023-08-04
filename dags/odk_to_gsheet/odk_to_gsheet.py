import datetime
import os
import shutil
from zipfile import ZipFile

import pendulum
import requests
from requests.auth import HTTPBasicAuth

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.google.cloud.transfers.local_to_gcs import \
    LocalFilesystemToGCSOperator
from airflow.providers.google.suite.transfers.gcs_to_sheets import \
    GCSToGoogleSheetsOperator

project_folder = os.path.join(os.getcwd(), 'dags', 'odk_to_gsheet')


@dag(
    dag_id="odk_to_gsheet",
    schedule="@daily",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    params={
        "form_name": Param(f'{Variable.get("form_name")}', type="string"),
        "project_id": Param(f'{Variable.get("project_id")}', type="string"),
        "base_url": Param(f'{Variable.get("base_url")}', type="string"),
        "username": Param(f'{Variable.get("username")}', type="string", format="idn-email"),
        "password": Param(f'{Variable.get("password")}', type="string"),
    },

)
def ODKtoGSheet():
    def create_gcs_pairs(filename: str):
        gcs_pairs = {
            "src": os.path.join(project_folder, 'data', f'{Variable.get("form_name")}', filename),
            "dst": filename
        }
        return gcs_pairs

    def create_gsheet_pairs(filename: str):
        gcs_pairs = {
            "object_name": filename,
            "spreadsheet_range": filename.replace(".csv", "")
        }
        return gcs_pairs

    @task
    def list_data():
        return [file
                for file in os.listdir(os.path.join(project_folder, 'data', Variable.get("form_name")))
                if os.path.splitext(file)[1] == '.csv']

    list_data_res = list_data()

    @task
    def get_data():
        for folder in ['download', 'data']:
            if not os.path.exists(os.path.join(project_folder, folder)):
                os.makedirs(os.path.join(project_folder, folder), exist_ok=True)
        data_dir = os.path.join(project_folder, 'data', f'{Variable.get("form_name")}')
        api_url = f'{Variable.get("base_url")}/v1/projects/{Variable.get("project_id")}/forms/{Variable.get("form_name")}/submissions.csv.zip?groupPaths=false'
        filename = os.path.join(project_folder, 'download', f'{Variable.get("form_name")}.zip')

        res = requests.get(api_url, stream=True, verify=False, auth=HTTPBasicAuth(
            f'{Variable.get("username")}', f'{Variable.get("password")}'), timeout=60)
        if res.status_code == 200:
            with open(filename, 'wb') as fh:
                res.raw.decode_content
                shutil.copyfileobj(res.raw, fh)

        with ZipFile(filename, 'r') as zip_file:
            zip_file.extractall(path=data_dir)

    gcs_pairs = list_data_res.map(create_gcs_pairs)

    upload_to_gcs = LocalFilesystemToGCSOperator.partial(
        task_id="upload_to_gcs",
        bucket='imperativa-airflow',
        gcp_conn_id='google_cloud_default'
    ).expand_kwargs(gcs_pairs)

    gsheet_pairs = list_data_res.map(create_gsheet_pairs)

    create_google_sheet = GCSToGoogleSheetsOperator.partial(
        task_id="create_google_sheet",
        spreadsheet_id='16UNWYGmREUsINRElbxWQ8ViSmsY_R7vrPv0DDPkxlqg',
        bucket_name='imperativa-airflow',
        gcp_conn_id='google_cloud_default',
    ).expand_kwargs(gsheet_pairs)

    get_data() >> list_data_res
    upload_to_gcs >> create_google_sheet


dag = ODKtoGSheet()
