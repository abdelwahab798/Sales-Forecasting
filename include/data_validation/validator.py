from utils.minio_s3 import define_s3_hook,read_from_s3,validate_config,load_config
import pandas as pd
import io
import os

class Datavalidator:
    def __init__(self,config_path=os.getenv("MLML_CONFIG_LOCAL")):
        valid_config=validate_config(config_path)
        self.config=load_config(valid_config)

        





















""""

        for i , sales_file in enumerate(file_paths["sales"][:10]):
            obj = s3_hook.get_key(key=sales_file, bucket_name=bucket)
            parquet_buffer = io.BytesIO(obj.get()['Body'].read())
            df_sales = pd.read_parquet(parquet_buffer)
            if i==0:
                print(f"Sample data from {df_sales.shape[0]} rows:")
                print(df_sales.head(1))
                print(f"Columns: {df_sales.columns.tolist()}")
            if df_sales.empty:
                issues.append(f"File {sales_file} is empty.")
                continue
            total_rows+=len(df_sales)
            required_columns=["date","product_id", "store_id", "quantity_sold", "revenue_egp"]
            missing_columns=set(required_columns)-set(df_sales.columns)
            if missing_columns:
                issues.append(f"File {sales_file} is missing columns: {missing_columns}")

            if df_sales["quantity_sold"].min() < 0:
                issues.append(f"File {sales_file} has negative values in 'quantity_sold' column.")
            if df_sales["revenue_egp"].min() < 0:
                issues.append(f"File {sales_file} has negative values in 'revenue_egp' column.")
            if df_sales["date"].isnull().any():
                issues.append(f"File {sales_file} has null values in 'date' column.")
            if df_sales["product_id"].isnull().any():
                issues.append(f"File {sales_file} has null values in 'product_id' column.")
            if df_sales["store_id"].isnull().any():
                issues.append(f"File {sales_file} has null values in 'store_id' column.")

        for data_type in ["customer_traffic","inventory","promotions","store_events"]:
            for file in file_paths[data_type][:2]:
                obj=s3_hook.get_key(key=file, bucket_name=bucket)
                parquet_buffer=io.BytesIO(obj.get()['Body'].read())
                df = pd.read_parquet(parquet_buffer)
                if df.empty:
                    issues.append(f"File {file} is empty.")

        summary={
            "total_files_validated": len(file_paths.get("sales", [])),
            "total_rows_validated": total_rows,
            "number_of_issues": len(issues),
            "issues": issues[:2]
        }
        return summary
        """"