from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from datetime import datetime
from minio import Minio


def _get_minio_connection():
    conn = BaseHook.get_connection("minio_connection")
    minio_endpoint = conn.host  # Should include http:// or https://
    access_key = conn.login
    secret_key = conn.password
    minio_client = Minio(
        endpoint=f"{minio_endpoint}:9000",
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )
    return minio_client

