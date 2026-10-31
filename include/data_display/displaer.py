from include.logger.main_logger import get_logger
from include.utils.minio_s3 import read_from_s3
logger = get_logger("data_display")

def display_data(data):
    bucket=data["bucket"]
    file_paths=data["file_paths"]
    for category, paths in file_paths.items():
        if not paths:
            logger.info(f"No files found for category: {category}")
            continue
        try:
            sample_paths=paths[0]
            df=read_from_s3(sample_paths,bucket)
            logger.info("=" * 50)
            logger.info(f"File: {sample_paths}")
            logger.info(f"Category: {category.upper()}")
            logger.info(f"Columns: {df.columns.tolist()}")
            logger.info(f"Shape: {df.shape} (Rows x Columns)")
            logger.info(f"Description:\n{df.describe(include='all')}")
            logger.info(f"data types\n {df.dtypes}")
            logger.info("\n--- Sample Row ---")
            logger.info(df.head(5))
            logger.info("=" * 50 + "\n")
        except Exception as e:
            logger.error(f"Error displaying data for category: {category}. Error: {e}")
    