import sys
import os
import logging
import json
import requests
import io
from pendulum import datetime
import pandas as pd
import boto3
from minio.error import S3Error
from minio import Minio
from astro.sql.table import Table
from astro import sql as aql
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.decorators import dag, task, task_group


# current_dir = os.path.dirname(__file__)
# print(current_dir)
# project_root = os.path.abspath(os.path.join(current_dir, "../"))
# print(project_root)
# sys.path.append(project_root)
# print(sys.path)

# from modules.airflow_connections import _get_minio_connection
# from airc.modules.airflow_connections import _get_minio_connection
from airc.modules.data_processing import *

# MinIO buckets


BUCKET_NAME = "canada-housing"
# DROP_UNUSE_OBJECT_PATH = "data/intermediate/files/drop_unuse.csv"
INTERMEDIATE_OBJECT_PATH = "data/intermediate/files"
PROCESSED_OBJECT_PATH = "data/processed/files"
FINAL_OBJECT_PATH = "data/final/files"

logger = logging.getLogger(__name__)

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 2},
    tags=["canada-dev"],
)
def canada_housing():

    @task.bash
    def test_minio_connection() -> str:
        return "curl -I http://192.168.88.15:9000/minio/health/live"

    @task
    def merge_all_provinces():
        """
        Get objects from MinIO bucket with pre-defined bucket and prefix
        Concatenate into one file
        Push back to MinIO
        """
        RAW_PATH = "data/raw/files"
        CONCAT_OUTPUT_FILE_PATH = "/tmp/s1_concatenated.csv"
        concatenated_file = concatenate_files(BUCKET_NAME, RAW_PATH, axis=0)
        concatenated_file.to_csv(CONCAT_OUTPUT_FILE_PATH, index=False)
        put_file_to_minio(BUCKET_NAME,
                    INTERMEDIATE_OBJECT_PATH + "/s1_concatenated.csv",
                    CONCAT_OUTPUT_FILE_PATH)
        
        return f"File exported at {CONCAT_OUTPUT_FILE_PATH}"

    @task
    def remove_unused_records_and_attributes():
        DROP_UNUSED_OUTPUT_FILE_PATH = "/tmp/s2_drop_unused.csv"
        response = get_minio_object(BUCKET_NAME,
                                    INTERMEDIATE_OBJECT_PATH + "/s1_concatenated.csv")
        df = minio_object_to_dataframe(response)
        processed_df = exclude_unused_data(df)
        logger.info(processed_df.head())
        processed_df.to_csv(DROP_UNUSED_OUTPUT_FILE_PATH, index=False)
        
        put_file_to_minio(BUCKET_NAME,
                        INTERMEDIATE_OBJECT_PATH + "/s2_drop_unused.csv",
                        DROP_UNUSED_OUTPUT_FILE_PATH)
        logger.info("Finished remove unused data and pushed to MinIO")
        return 0
    
    @task_group
    def interior():
        @task
        def fill_garage_missing_values():
            # Lấy object
            response = get_minio_object(BUCKET_NAME, 
                                        INTERMEDIATE_OBJECT_PATH + "/s2_drop_unused.csv")
            processed_df = process_garage(response)
            OUTPUT_PATH = "/tmp/processed_garage.csv"
            processed_df.to_csv(OUTPUT_PATH, index=False)
            put_file_to_minio(BUCKET_NAME, 
                            PROCESSED_OBJECT_PATH + "/s3_processed_garage.csv", 
                            OUTPUT_PATH)
            logger.info("Successfully fill missing values for Garage and pushed to MinIO")
            return 0
        
        @task
        def create_new_flooring_feature():
            response = get_minio_object(BUCKET_NAME, 
                                        INTERMEDIATE_OBJECT_PATH + "/s2_drop_unused.csv")
            flooring_df = transform_flooring(response)
            OUTPUT_PATH = "/tmp/processed_flooring.csv"
            flooring_df.to_csv(OUTPUT_PATH, index=False)
            put_file_to_minio(BUCKET_NAME,
                            PROCESSED_OBJECT_PATH + "/s3_processed_flooring.csv",
                            OUTPUT_PATH)
            logger.info("Successfully create new value for Flooring and pushed to MinIO")
            return 0
        
        t1 = fill_garage_missing_values()
        t2 = create_new_flooring_feature()
        [t1 >> t2]

    @task_group
    def exterior():
        @task
        def fill_acrage_missing_values():
            response = get_minio_object(BUCKET_NAME, 
                                        INTERMEDIATE_OBJECT_PATH + "/s2_drop_unused.csv")
            
            processed_df = process_acreage(response)
            OUTPUT_PATH = "/tmp/processed_acreage.csv"
            processed_df.to_csv(OUTPUT_PATH, index=False)
            put_file_to_minio(BUCKET_NAME, 
                            PROCESSED_OBJECT_PATH + "/s3_processed_acreage.csv", 
                            OUTPUT_PATH)
            logger.info("Successfully fill missing values for Acreage and pushed to MinIO")
            
            return 0
        
        @task
        def create_new_basement_features():
            response = get_minio_object(BUCKET_NAME, 
                                        INTERMEDIATE_OBJECT_PATH + "/s2_drop_unused.csv")
            basement_df = transform_basement(response)
            OUTPUT_PATH = "/tmp/processed_basement.csv"
            basement_df.to_csv(OUTPUT_PATH, index=False)
            put_file_to_minio(BUCKET_NAME, 
                            PROCESSED_OBJECT_PATH + "/s3_processed_basement.csv", 
                            OUTPUT_PATH)
            logger.info("Successfully create new value for Basement and pushed to MinIO")

            return 0
        
        @task
        def create_new_roof_feature():
            response = get_minio_object(BUCKET_NAME, 
                                        INTERMEDIATE_OBJECT_PATH + "/s2_drop_unused.csv")
            roof_df = transform_roof(response)
            OUTPUT_PATH = "/tmp/processed_basement.csv"
            roof_df.to_csv(OUTPUT_PATH, index=False)
            put_file_to_minio(BUCKET_NAME, 
                            PROCESSED_OBJECT_PATH + "/s3_processed_basement.csv", 
                            OUTPUT_PATH)
            logger.info("Successfully create new value for Basement and pushed to MinIO")

            return 0
        
        t1 = fill_acrage_missing_values()
        t2 = create_new_basement_features()
        t3 = create_new_roof_feature()
        [t1, t2, t3]
    
    @task_group
    def amentity():
        @task
        def create_new_heating_feature():
            response = get_minio_object(BUCKET_NAME, 
                                        INTERMEDIATE_OBJECT_PATH + "/s2_drop_unused.csv")
            heating_df = transform_heating(response)
            OUTPUT_PATH = "/tmp/processed_heating.csv"
            heating_df.to_csv(OUTPUT_PATH, index=False)
            put_file_to_minio(BUCKET_NAME, 
                            PROCESSED_OBJECT_PATH + "/s3_processed_heating.csv", 
                            OUTPUT_PATH)
            logger.info("Successfully create new value for Heating and pushed to MinIO")

            return 0
        
        @task
        def create_new_waterfront_feature():
            response = get_minio_object(BUCKET_NAME, 
                                        INTERMEDIATE_OBJECT_PATH + "/s2_drop_unused.csv")
            waterfront_df = transform_waterfront(response)
            OUTPUT_PATH = "/tmp/processed_waterfront.csv"
            waterfront_df.to_csv(OUTPUT_PATH, index=False)
            put_file_to_minio(BUCKET_NAME, 
                            PROCESSED_OBJECT_PATH + "/s3_processed_waterfront.csv", 
                            OUTPUT_PATH)
            logger.info("Successfully create new value for Waterfront and pushed to MinIO")

            return 0
        
        @task
        def create_new_sewer_feature():
            response = get_minio_object(BUCKET_NAME, 
                                        INTERMEDIATE_OBJECT_PATH + "/s2_drop_unused.csv")
            sewer_df = transform_sewer(response)
            OUTPUT_PATH = "/tmp/processed_sewer.csv"
            sewer_df.to_csv(OUTPUT_PATH, index=False)
            put_file_to_minio(BUCKET_NAME, 
                            PROCESSED_OBJECT_PATH + "/s3_processed_sewer.csv", 
                            OUTPUT_PATH)
            logger.info("Successfully create new value for Sewer and pushed to MinIO")

            return 0
        
        t1 = create_new_heating_feature()
        t2 = create_new_waterfront_feature()
        t3 = create_new_sewer_feature()
        
        [t1, t2, t3]
    
    @task_group
    def add_new_house_features():
        """This function generate new 4 features: Pool, Garden, View, Balcony
        """
        @task
        def combined_features():
            response = get_minio_object(BUCKET_NAME, 
                                        INTERMEDIATE_OBJECT_PATH + "/s2_drop_unused.csv")
            new_features_df = add_new_features(response)
            OUTPUT_PATH = "/tmp/processed_new_features.csv"
            new_features_df.to_csv(OUTPUT_PATH, index=False)
            put_file_to_minio(BUCKET_NAME, 
                            INTERMEDIATE_OBJECT_PATH + "/s3_processed_new_features.csv", 
                            OUTPUT_PATH)
            logger.info("Successfully create new features and pushed to MinIO")
            return 0    

        @task
        def create_pool_from_combined():
            response = get_minio_object(BUCKET_NAME, 
                                        INTERMEDIATE_OBJECT_PATH + "/s3_processed_new_features.csv")
            OUTPUT_PATH = "/tmp/processed_pool.csv"
            pool_df = add_pool(response)
            pool_df.to_csv(OUTPUT_PATH, index=False)
            put_file_to_minio(BUCKET_NAME, 
                            PROCESSED_OBJECT_PATH + "/s3_processed_pool.csv", 
                            OUTPUT_PATH)
            return 0
        
        @task
        def create_garden_from_combined():
            response = get_minio_object(BUCKET_NAME, 
                                        INTERMEDIATE_OBJECT_PATH + "/s3_processed_new_features.csv")
            OUTPUT_PATH = "/tmp/processed_garden.csv"
            garden_df = add_garden(response)
            garden_df.to_csv(OUTPUT_PATH, index=False)
            put_file_to_minio(BUCKET_NAME, 
                            PROCESSED_OBJECT_PATH + "/s3_processed_garden.csv", 
                            OUTPUT_PATH)
            
            return 0
        
        @task
        def create_view_from_combined():
            response = get_minio_object(BUCKET_NAME, 
                                        INTERMEDIATE_OBJECT_PATH + "/s3_processed_new_features.csv")
            OUTPUT_PATH = "/tmp/processed_view.csv"
            view_df = add_view(response)
            view_df.to_csv(OUTPUT_PATH, index=False)
            put_file_to_minio(BUCKET_NAME, 
                            PROCESSED_OBJECT_PATH + "/s3_processed_view.csv", 
                            OUTPUT_PATH)
            return 0
        
        @task
        def create_balcony_from_combined():
            response = get_minio_object(BUCKET_NAME, 
                                        INTERMEDIATE_OBJECT_PATH + "/s3_processed_new_features.csv")
            OUTPUT_PATH = "/tmp/processed_balcony.csv"
            balcony_df = add_balcony(response)
            balcony_df.to_csv(OUTPUT_PATH, index=False)
            put_file_to_minio(BUCKET_NAME, 
                            PROCESSED_OBJECT_PATH + "/s3_processed_balcony.csv", 
                            OUTPUT_PATH)
            return 0
        
        t1 = combined_features()
        t2 = create_pool_from_combined()
        t4 = create_garden_from_combined()
        t3 = create_view_from_combined()
        t5 = create_balcony_from_combined()
        
        [t1 >> [t2, t3, t4, t5]]
        
    @task
    def join_data_from_previous_steps():
        """Function này dùng để đọc tất cả các file
        trong bucket có prefix là s3 và join lại với nhau
        Bây giờ phải check 1 lượt xem là các cột dữ liệu
        đơn lẻ kia đã theo đúng định dạng chưa
        """
        OUTPUT_PATH = "/tmp/s4_new_concatenated_file.csv"
        features_df = join_data(BUCKET_NAME, PROCESSED_OBJECT_PATH)
        print(f'This is feature_df {features_df.columns}')
        response = get_minio_object(BUCKET_NAME, 
                            INTERMEDIATE_OBJECT_PATH + "/s2_drop_unused.csv")
        drop_unused_df = minio_object_to_dataframe(response)
        
        concatenated_df = pd.concat([drop_unused_df, features_df], ignore_index=False, axis=1)
        concatenated_df.to_csv(OUTPUT_PATH, index=False)
        put_file_to_minio(BUCKET_NAME, 
                INTERMEDIATE_OBJECT_PATH + "/s4_new_concatenated_file.csv",
                OUTPUT_PATH)
        return 0
    
    @task
    def final_clean():
        response = get_minio_object(BUCKET_NAME,
                                    INTERMEDIATE_OBJECT_PATH + "/s4_new_concatenated_file.csv")
        df = minio_object_to_dataframe(response)
        df = drop_old_columns(df)
        df = rename_column(df)
        df['Square_Footage'] = df['Square_Footage'].str.replace(',','')
        number_columns = ['Latitude', 'Longitude', 'Price', 'Bedrooms', 
                        'Bathrooms', 'Acreage', 'Square_Footage']

        for col in number_columns: 
            df[col] = df[col].astype(float)

        logger.info(df.columns)
        df.dropna(subset=["Bedrooms", "Bathrooms", "Square_Footage", "City", "Province"], inplace=True)
        
        # Data rule for Square_Footage
        df = df[df["Square_Footage"] > 120]
        # Data rule for Price
        df = df[df.Price >= 50_000]
        
        OUTPUT_PATH = "/tmp/s5_final.csv"
        df.to_csv(OUTPUT_PATH, index=False)
        put_file_to_minio(BUCKET_NAME, 
                FINAL_OBJECT_PATH + "/s5_final.csv", 
                OUTPUT_PATH)
        return 0
    
    task_1 = test_minio_connection()
    task_2 = merge_all_provinces()
    task_3 = remove_unused_records_and_attributes()
    task_4 = interior()
    task_5 = exterior()
    task_6 = amentity()
    task_7 = add_new_house_features()
    task_8 = join_data_from_previous_steps()
    task_9 = final_clean()
    
    # Define task dependency
    task_1 >> task_2 >> task_3 >> [task_4, task_5, task_6, task_7] >> task_8 >> task_9

canada_housing_dag = canada_housing()
