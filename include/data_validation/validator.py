from include.utils.minio_s3 import validate_config,load_config,read_from_s3
from include.data_validation.schema import build_pandera_schema
import pandas as pd
import pandera as pa
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
            table_rules = self.data_types.get(table_name, {})
            paths=file_paths[table_name][:10]
            if not paths:
                errors.append(f"No files found for table: {table_name}")
                continue
            df=pd.concat([read_from_s3(p,bucket) for p in paths],ignore_index=True)
            required_cols=self.required_columns.get(table_name,[])
            mssing=set(required_cols)-set(df.columns)
            if mssing:
                errors.append(f"Missing required columns in table: {table_name}, missing columns: {mssing}")
                
            NaN=df.isnull().sum()
            Duplicates=df.duplicated().sum()
            if int(NaN.sum())>0:
                errors.append(f"Table {table_name} has {int(NaN.sum())} NaN values")
            if int(Duplicates)>0:
                errors.append(f"Table {table_name} has {int(Duplicates)} duplicate rows")

            schema=build_pandera_schema(table_rules=table_rules,allow_nulls=False)  
            try:      
                schema.validate(df,lazy=True)
            except pa.errors.SchemaErrors as err:
                failure_summary = (
                    err.failure_cases.groupby(["column", "check"], dropna=False)
                    .size()
                    .reset_index(name="error_count")
                )
                for _,row in failure_summary.iterrows():
                    err_msg=(f"Table '{table_name}' -> Column '{row['column']}' "
                            f"failed check '{row['check']}' ({row['error_count']} cases)")
                    errors.append(err_msg)
                    logger.error(err_msg)
            logger.info(f"Basic validation completed for table {table_name}. Errors found:{len(errors)-before_errors} ")
        return errors


    