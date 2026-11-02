from datetime import datetime, timedelta
import gc
from airflow.decorators import dag, task
import pandas as pd
from include.data_extraction.extractor import extract_data
from include.data_display.displaer import display_many_data_files,display_merged_data
from include.feature_engineering.engineer import engineer_features
from include.data_processing.mergeing import merge_data
from include.data_validation.validator import Datavalidator
from airflow.exceptions import AirflowFailException
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args ={
    'owner': 'airflow',
    'depends_on_past': False,
    "start_date": datetime(2026,8,27),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

@dag(
    default_args=default_args,
    description="Sales Forecast Training DAG",
    tags=["ml","training","sales_forecast"],
    schedule="@weekly",
)
def sales_forecast_training():
    @task()
    def extract():
         return extract_data()
    @task()
    def display(extracted_data):
         display_many_data_files(extracted_data)

    @task()
    def basic_validation_data(extracted_data):
         erros=Datavalidator().basic_validate(extracted_data)
         if len(erros)>0:
             logger.error(f"Data validation failed with errors: {erros}")
             raise AirflowFailException(f"Data validation failed with errors: {erros}")
         return extracted_data
    @task()
    def merge(validate_data):
       return merge_data(validate_data)
    @task()
    def display_data_after_merged(merged_data):
        display_merged_data(merged_data)

    @task()
    def build_feature_engineering(merged_data):
        return engineer_features(merged_data)

    @task()
    def display_feature_engineered_data(feature_engineered_data):
        display_merged_data(feature_engineered_data,Category="FEATURE_ENGINEERED_DATA")

    

         
    
    orignal_data=extract()
    display(orignal_data)
    validate_data=basic_validation_data(orignal_data)
    mergeded_data=merge(validate_data)
    display_data_after_merged(mergeded_data)
    feature_engineered_data=build_feature_engineering(mergeded_data)
    display_feature_engineered_data(feature_engineered_data)

sales_forecast_training_dag=sales_forecast_training()
         
         
    

    
"""
    @task(execution_timeout=timedelta(minutes=10))
    def merge_data(extracted_data):
        s3_hook = S3Hook(aws_conn_id="minio_s3")
        bucket=extracted_data["bucket"]
        output_path="merge_zone/merged_sales_data.parquet"
        con=duckdb.connect()

        con.execute(
            INSTALL httpfs; LOAD httpfs;
        SET s3_endpoint='minio:9000';
        SET s3_access_key_id='minioadmin';
        SET s3_secret_access_key='minioadmin';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
        )
        logger.info(f"setup duckdb connection to s3://{bucket}/{output_path}")

        con.execute(f"CREATE VIEW sales AS SELECT * FROM read_parquet('s3://{bucket}/raw_zone/sales/**/*.parquet');")

        con.execute(f"CREATE VIEW inventory AS SELECT product_id, store_id, date, inventory_level, reorder_point, days_of_supply FROM read_parquet('s3://{bucket}/raw_zone/inventory/**/*.parquet');")

        con.execute(f"CREATE VIEW promotions AS SELECT product_id,date, promotion_type, discount_percent FROM read_parquet('s3://{bucket}/raw_zone/promotions/**/*.parquet');")

        con.execute(f"CREATE VIEW store_events AS SELECT store_id, date, event_type, impact FROM read_parquet('s3://{bucket}/raw_zone/store_events/**/*.parquet');")

        con.execute(f"CREATE VIEW customer_traffic AS SELECT store_id, date, customer_traffic, weather_impact, is_holiday FROM read_parquet('s3://{bucket}/raw_zone/customer_traffic/**/*.parquet');")
        
        merge_query=f
        Copy(
        select s.*,
        i.inventory_level,i.reorder_point,i.days_of_supply,
        p.promotion_type,p.discount_percent,
        e.event_type,e.impact,
        c.customer_traffic,c.weather_impact,c.is_holiday
        from Sales s

        left join inventory i
        on s.product_id=i.product_id and s.store_id=i.store_id and s.date=i.date

        left join promotions p
        on s.product_id=p.product_id and s.date=p.date

        left join store_events e
        on s.store_id=e.store_id and s.date=e.date

        left join customer_traffic c
        on s.store_id=c.store_id and s.date=c.date
    ) To 's3://{bucket}/{output_path}' (FORMAT PARQUET);
       
        logger.info(f"Executing merge query for s3://{bucket}/{output_path}")
        con.execute(merge_query)
        con.close()
        return {"bucket": bucket, "output_path": output_path}

    @task()
    def display_data_after_merged(merged_data):
        s3_hook = S3Hook(aws_conn_id="minio_s3")
        bucket=merged_data["bucket"]
        output_path=merged_data["output_path"]
        obj=s3_hook.get_key(key=output_path, bucket_name=bucket)
        parquet_buffer=io.BytesIO(obj.get()['Body'].read())
        df=pd.read_parquet(parquet_buffer)
        print("=" * 50)
        print(f"Merged Data Sample:")
        print(f"Shape: {df.shape} (Rows x Columns)")
        print(f"Columns: {df.columns.tolist()}")
        print("\n--- Sample Row ---")
        print(df.head(5))
        print("=" * 50 + "\n")
        print(f"nan values per column:\n{df.isna().sum()}")
        print(f"Duplicate Rows: {df.duplicated().sum()}")
        print(f"Data Types:\n{df.dtypes}")
        print(f"Summary Statistics:\n{df.describe(include='all')}")

    @task()
    def preprocessing_merged_data(merged_data):
            s3_hook = S3Hook(aws_conn_id="minio_s3")
            bucket=merged_data["bucket"]
            output_path=merged_data["output_path"]
            obj=s3_hook.get_key(key=output_path, bucket_name=bucket)
            file_output="processed_zone/preprocessed_sales_data.parquet"
            df=pd.read_parquet(io.BytesIO(obj.get()['Body'].read()))

            df = df.sort_values(['store_id', 'product_id', 'date'])
            df.drop(columns=["discount_percent_1"], inplace=True)
            df["promotion_type"]=df["promotion_type"].fillna("No Promotion")
            df["discount_percent"]=df["discount_percent"].fillna(1)
            df["event_type"]=df["event_type"].fillna("No Event")
            df["impact"]=df["impact"].fillna(1)
            inv_cols = ['inventory_level', 'reorder_point', 'days_of_supply']
            df[inv_cols]=df.groupby(['store_id', 'product_id'])[inv_cols].ffill().fillna(0)
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            s3_hook.load_bytes(bytes_data=buffer.getvalue(),key=file_output,bucket_name=bucket,replace=True)
            return {"bucket": bucket, "file_output": file_output}
    @task()
    def display_preprocessed_data(preprocessed_data):
        s3_hook = S3Hook(aws_conn_id="minio_s3")
        bucket=preprocessed_data["bucket"]
        file_output=preprocessed_data["file_output"]
        obj=s3_hook.get_key(key=file_output, bucket_name=bucket)
        df=pd.read_parquet(io.BytesIO(obj.get()['Body'].read()))
        print("=" * 50)
        print(f"Preprocessed Data Sample:")
        print(f"Shape: {df.shape} (Rows x Columns)")
        print(f"Columns: {df.columns.tolist()}")
        print("\n--- Sample Row ---")
        print(df.head(5))
        print("=" * 50 + "\n")
        print(f"nan values per column:\n{df.isna().sum()}")
        print(f"Duplicate Rows: {df.duplicated().sum()}")
        print(f"Data Types:\n{df.dtypes}")
        print(f"Summary Statistics:\n{df.describe(include='all')}")

    @task()
    def Feature_engineering(merged_data):
        s3_hook = S3Hook(aws_conn_id="minio_s3")
        bucket=merged_data["bucket"]
        file_output=merged_data["file_output"]
        new_file_output="feature_engineered_zone/feature_engineered_sales_data.parquet"
        obj=s3_hook.get_key(key=file_output, bucket_name=bucket)
        df=pd.read_parquet(io.BytesIO(obj.get()['Body'].read()))

        df["day"]=df["date"].dt.day
        df["month"]=df["date"].dt.month
        df["year"]=df["date"].dt.year
        df["day_of_week"]=df["date"].dt.dayofweek
        df["quarter"]=df["date"].dt.quarter
        df["is_month_start"]=df["date"].dt.is_month_start
        df["is_month_end"]=df["date"].dt.is_month_end

        cate_colums=["store_id","product_id","promotion_type", "event_type"]
        for col in cate_colums:
             df[col]=df[col].astype("category").cat.codes

        grouped=df.groupby(['store_id', 'product_id'])['quantity_sold']

        df['quantity_lag_1'] =grouped.shift(1)
        df['quantity_lag_7'] =grouped.shift(7)
        df['quantity_lag_14'] =grouped.shift(14)
        df['quantity_lag_28'] =grouped.shift(28)

        df["quantity_roll_mean_7"]=grouped.transform(lambda x: x.shift(1).rolling(7).mean())
        df["quantity_roll_mean_28"]=grouped.transform(lambda x: x.shift(1).rolling(28).mean())
        df["quantity_roll_std_7"]=grouped.transform(lambda x: x.shift(1).rolling(7).std())

        df.drop(columns=['unit_price_egp', 'revenue_egp', 'cost_egp', 'profit_egp'],inplace=True)
        df.dropna(inplace=True)

        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)

        print(f"Shape: {df.shape} (Rows x Columns)")
        print(f"Columns: {df.columns.tolist()}")
        print("\n--- Sample Row ---")
        print(df.head(5))
        print("=" * 50 + "\n")
        print(f"nan values per column:\n{df.isna().sum()}")
        print(f"Duplicate Rows: {df.duplicated().sum()}")
        print(f"Data Types:\n{df.dtypes}")
        print(f"Summary Statistics:\n{df.describe(include='all')}")

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        s3_hook.load_bytes(bytes_data=buffer.getvalue(),key=new_file_output,bucket_name=bucket,replace=True)
        return {"bucket": bucket, "file_output": new_file_output}

    @task()
    def train_model(feature_engineered_data):
        s3_hook = S3Hook(aws_conn_id="minio_s3")
        bucket=feature_engineered_data["bucket"]
        file_output=feature_engineered_data["file_output"]
        obj=s3_hook.get_key(key=file_output, bucket_name=bucket)
        df=pd.read_parquet(io.BytesIO(obj.get()['Body'].read()))
        

        



    extracted_data=extract_data()
    display_extracted_data(extracted_data)
    summary=validation_data(extracted_data)
    print(f"Validation Summary: {summary}")
    merged_data=merge_data(extracted_data)
    display_data_after_merged(merged_data)
    preprocessed_data=preprocessing_merged_data(merged_data)
    display_preprocessed_data(preprocessed_data)
    feature_engineered_data=Feature_engineering(preprocessed_data)
sales_forecast_training_dag=sales_forecast_training()
"""