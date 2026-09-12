from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.logging.logger import get_logger
import pandas as pd
import io
import yaml
logger=get_logger("minio_s3")


def read_from_s3(path,bucket):
    s3_hook = define_s3_hook()
    obj = s3_hook.get_key(key=path, bucket_name=bucket)
    df=pd.read_parquet(io.BytesIO(obj.get()['Body'].read()))
    return df


def upload_to_s3(df, path, bucket):
    s3_hook =define_s3_hook()
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    s3_hook.load_bytes(bytes_data=buffer.getvalue(), key=path, bucket_name=bucket, replace=True)

def define_s3_hook():
    s3_hook = S3Hook(aws_conn_id='minio_s3')
    return s3_hook

def load_config(config_path):
    with open(config_path,"r") as file:
        config=yaml.safe_load(file)
    return config

def validate_config(config_path):
    if config_path is None:
        logger.error("ML_CONFIG_LOCAL environment variable is not set")
        raise ValueError("ML_CONFIG_LOCAL environment variable is not set")
    return config_path



        
