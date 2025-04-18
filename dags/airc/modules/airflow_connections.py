from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from datetime import datetime
from minio import Minio
from sqlalchemy import create_engine, text

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

def get_postgres_connection(db_name, user='username', password='password', host='localhost', port=5432):
    """
    Attempts to connect to the specified PostgreSQL database and runs a test query.

    Returns:
    - engine if successful
    - None if failed
    """
    try:
        db_url = f'postgresql://{user}:{password}@{host}:{port}/{db_name}'
        engine = create_engine(db_url)
        
        # Connect and run a simple query
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        print(f"✅ Successfully connected to database: {db_name}")
        return engine
    except Exception as e:
        print(f"❌ Failed to connect to database: {db_name}")
        print(f"Error: {e}")
        return None
