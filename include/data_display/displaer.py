from include.logger.main_logger import get_logger
from include.utils.minio_s3 import read_from_s3
logger = get_logger("data_display")

def display_data(bucket:str,file_pahts:str,category:str="MERGED_DATA"):
    if not file_pahts:
        logger.info(f"No files found for category: {category}")
        return
    try:
        df=read_from_s3(file_pahts,bucket)
        logger.info("=" * 50)
        logger.info(f"File: {file_pahts}")
        logger.info(f"Category: {category.upper()}")
        logger.info(f"Columns: {df.columns.tolist()}")
        logger.info(f"Shape: {df.shape}")
        num_desc = df.describe().T.round(2).to_markdown()
        logger.info(f"Numeric Description:\n{num_desc}")
        cat_desc = df.describe(include=['object', 'bool']).T.to_markdown()
        logger.info(f"Categorical Description:\n{cat_desc}")
        logger.info(f"data types\n {df.dtypes}")
        logger.info("\n--- Sample Row ---")
        logger.info(df.head(5))
        logger.info("---- NaN or Duplicates")
        logger.info(f"Number of NaN values:\n{df.isna().sum()}")
        logger.info(f"Number of duplicate rows: {df.duplicated().sum()}")
        logger.info("=" * 50 + "\n")
    except Exception as e:
        logger.error(f"Error displaying data for category: {category}. Error: {e}")


def display_many_data_files(data:dict):
    bucket=data["bucket"]
    file_paths=data["file_paths"]
    for category, paths in file_paths.items():
        if not paths:
            logger.info(f"No files found for category: {category}")
            continue
        display_data(bucket, paths[0], category)


def display_merged_data(data:dict,Category:str="MERGED_DATA"):
    bucket=data["bucket"]
    file_paths=data["file_paths"]
    display_data(bucket, file_paths, "MERGED_DATA")
    
    
    