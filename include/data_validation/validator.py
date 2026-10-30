from include.utils.minio_s3 import validate_config,load_config,read_from_s3
import pandas as pd
import os
from include.logger.main_logger import get_logger
logger=get_logger("data_validation")

class Datavalidator:
    def __init__(self,config_path=os.getenv("ML_CONFIG_LOCAL")):
        valid_config=validate_config(config_path)
        self.config=load_config(valid_config)
        self.validation_rules=self.config.get("validation")
        self.required_columns=self.validation_rules.get("required_columns")
        self.data_tables=self.config.get("data_tables")
        self.data_types=self.validation_rules.get("data_types")


    def basic_validate(self,extract_data):
        errors=[]
        tables=self.data_tables.keys()
        file_paths=extract_data["file_paths"]
        bucket=extract_data["bucket"]
        for table_name in tables:
            before_errors=len(errors)
            paths=file_paths[table_name][:10]
            if not paths:
                errors.append(f"No files found for table: {table_name}")
                continue
            df=pd.concat(read_from_s3(p,bucket) for p in paths)        
            required_cols=self.required_columns.get(table_name)
            mssing=set(required_cols)-set(df.columns)
            if mssing:
                errors.append(f"Missing required columns in table: {table_name}, missing columns: {mssing}")
                
            data_types=self.data_types.get(table_name)
            NaN=df.isnull().sum()
            Duplicates=df.duplicated().sum()
            for col,excepted in data_types.items():
                if col in df.columns:
                    orginal=str(df[col].dtype)
                    if orginal != excepted:
                        errors.append(f"Column {col} has type {orginal}, expected {excepted} in table: {table_name}")
            if int(NaN.sum())>0:
                errors.append(f"Table {table_name} has {int(NaN.sum())} NaN values")
            if int(Duplicates)>0:
                errors.append(f"Table {table_name} has {int(Duplicates)} duplicate rows")
        
            logger.info(f"Basic validation completed for table {table_name}. Errors found:{len(errors)-before_errors} ")
        return errors


    