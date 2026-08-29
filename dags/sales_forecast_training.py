from datetime import datetime, timedelta
from airflow.decorators import dag, task
import pandas as pd

default_args ={
    'owner': 'airflow',
    'depends_on_past': False,
    "start_date": datetime(2026,8,27),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "schedule":"@weekly"
}

@dag(
    default_args=default_args,
    description="Sales Forecast Training DAG",
    tags=["ml","training","sales_forecast"]
)
def sales_forecast_training():
    @task()
    def extract_data():
        data_output_dir = "/tmp/sales_data"
        from include.utils.data_genrator import RealisticSalesDataGenerator

        genrator=RealisticSalesDataGenerator(start_date=datetime(2023, 1, 1), end_date=datetime(2025, 12, 31))
        print("Extracting data from the source...")
        file_paths=genrator.generate_sales_data(output_dir=data_output_dir)
        print(f"Data extracted and saved to {file_paths}")
        total_files = sum(len(paths) for paths in file_paths.values())
        print(f"Generated {total_files} files:")
        for data_type, paths in file_paths.items():
            print(f" - {data_type}: {len(paths)} files")
        return {
            "data_output_dir": data_output_dir,
            "file_paths": file_paths,
            "total_files": total_files,}
    @task()
    def validation_data(extracted_data):
        file_paths=extracted_data["file_paths"]
        issues = []
        total_rows=0

        for i , sales_file in enumerate(file_paths["sales_transactions"][:10]):
            df_sales=pd.read_parquet(sales_file)
            if i==0:
                print(f"Sample data from {df_sales.shape[0]} rows:")
                print(df_sales.head())
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
                df=pd.read_parquet(file)
                print(f"Sample data from {df.shape[0]} rows:")
                print(df.head())
                if df.empty:
                    issues.append(f"File {file} is empty.")

        validation_summary={
            "total_files_validated": len(file_paths["sales_transactions"]),
            "total_rows_validated": total_rows,
            "number_of_issues": len(issues),
            "issues": issues[:2]
        }
        return validation_summary


            

            
            
            
    



        
