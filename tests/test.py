from sqlalchemy import create_engine, text
from minio import Minio
import pandas as pd
import io


def test_db_connection(db_name, user='username', password='password', host='localhost', port=5432):
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



engine = test_db_connection(db_name="formatted_zone",
                                    user="airc",
                                    password="admin",
                                    host="192.168.88.146",
                                    port=5432)

df = pd.read_csv("/home/aircsrv5/Quan/DataOps/Data-for-testing/canada-house-price/archive-2/cleaned_canada.csv")
df.to_sql('canada_house_final', engine, if_exists='replace', index=False)



# minio_client = Minio(
#     endpoint="192.168.88.15:9000",
#     access_key='admin',
#     secret_key='password',
#     secure=False
# )
# BUCKET_NAME = "canada-housing"
# # DROP_UNUSE_OBJECT_PATH = "data/intermediate/files/drop_unuse.csv"
# INTERMEDIATE_OBJECT_PATH = "data/intermediate/files"
# PROCESSED_OBJECT_PATH = "data/processed/files"
# FINAL_OBJECT_PATH = "data/final/files"

# response = minio_client.get_object(bucket_name=BUCKET_NAME,
#                     object_name=FINAL_OBJECT_PATH + "/s5_final.csv")
# df = pd.read_csv(io.BytesIO(response.read()), low_memory=False)
# df.to_csv("/home/aircsrv5/Quan/DataOps/Data-for-testing/canada-house-price/s5_final.csv", index=False)