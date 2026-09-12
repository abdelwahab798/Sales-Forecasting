from include.utils.minio_s3 import define_s3_hook,load_config, validate_config
from include.logging.logger import get_logger
import os

logger=get_logger("data_extraction")

def extract_data(config_path=os.getenv("ML_CONFIG_LOCAL")):
        
        valid_config=validate_config(config_path)
        config=load_config(valid_config)
        s3_hook=define_s3_hook()
        bucket=config['buckets']['raw-data']
        categories = list(config['data_tables'].values())
        file_paths_map = {}
        for category in categories:
            prefix = f"raw_zone/{category}/"
            keys = s3_hook.list_keys(bucket_name=bucket, prefix=prefix)
            file_paths_map[category]=[k for k in keys if k.endswith(".parquet")]
        logger.info("Data extraction completed successfully.")
        return {"bucket": bucket, "file_paths": file_paths_map}