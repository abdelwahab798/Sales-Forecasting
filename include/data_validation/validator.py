from utils.minio_s3 import define_s3_hook,read_from_s3,validate_config,load_config
import pandas as pd
import io
import os
from include.logging.logger import get_logger
logger=get_logger("data_validation")

class Datavalidator:
    def __init__(self,config_path=os.getenv("MLML_CONFIG_LOCAL")):
        valid_config=validate_config(config_path)
        self.config=load_config(valid_config)
        self.validation_rules=self.config.get("validation")
        self.required_columns=self.validation_rules.get("required_columns")
        self.data_types=self.validation_rules.get("data_types")

    def validate_sales_data(self,df):
        errors=[]
        required_cols=self.required_columns.get("sales")
        data_types=self.data_types.get("sales")
        for col in required_cols:
            if col not in df.columns:
                errors.append(f"Missing required column: {col}")

        for col, expected_type in data_types.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)
                if actual_type != expected_type:
                    errors.append(f"Column {col} has type {actual_type}, expected {expected_type}")

        if df["revenue_egp"]-df["cost_egp"] != df["profit_egp"]:
            errors.append("Profit calculation mismatch: revenue_egp - cost_egp != profit_egp")
        if df["quantity"] < 0:
            errors.append("Quantity cannot be negative")
        if errors:
            logger.error(f"Sales data validation errors: {errors}")
            return False, errors
        



        
