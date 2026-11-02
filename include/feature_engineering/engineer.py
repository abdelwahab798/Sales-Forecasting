from include.logger.main_logger import get_logger
from include.utils.minio_s3 import read_from_s3,upload_to_s3
import os
from sklearn.preprocessing import OrdinalEncoder

encoder=OrdinalEncoder()
logger = get_logger("feature_engineering")

new_file_path=os.getenv("OUTPUT_PATH_FEATURE_ENGINEERING")
def engineer_features(data: dict):
    bucket = data["bucket"]
    file_paths = data["file_paths"]
    if not file_paths:
        logger.error("No merged data file found for feature engineering.")
        return None


    df=read_from_s3(file_paths, bucket)
    logger.info(f"Read merged data from s3://{bucket}/{file_paths}. Shape: {df.shape}")
    df=df.sort_values(['store_id', 'product_id', 'date'])
    logger.info("Starting feature engineering on merged data.")

    # fill inventory columns
    inv_cols = ['inventory_level', 'reorder_point', 'days_of_supply']
    df[inv_cols]=(df.groupby(['store_id', 'product_id'])[inv_cols].transform(lambda x: x.ffill().bfill()).fillna(0))  
    logger.info("Filled missing values in inventory columns using forward and backward fill.") 

    # Date features
    df["day"]=df["date"].dt.day
    df["month"]=df["date"].dt.month
    df["year"]=df["date"].dt.year
    df["day_of_week"]=df["date"].dt.dayofweek
    df["quarter"]=df["date"].dt.quarter
    df["is_month_start"]=df["date"].dt.is_month_start
    df["is_month_end"]=df["date"].dt.is_month_end
    logger.info("Extracted date features: day, month, year, day_of_week, quarter, is_month_start, is_month_end.")

    # encode categorical columns
    cate_colums=["store_id","product_id","promotion_type","event_type","category"]
    for col in cate_colums:
            df[col]=encoder.fit_transform(df[[col]])

    grouped=df.groupby(['store_id', 'product_id'])['quantity_sold']
    df['quantity_lag_1'] =grouped.shift(1)
    df['quantity_lag_7'] =grouped.shift(7)
    df['quantity_lag_14'] =grouped.shift(14)
    df['quantity_lag_28'] =grouped.shift(28)

    df["quantity_roll_mean_7"]=grouped.transform(lambda x: x.shift(1).rolling(7).mean())
    df["quantity_roll_mean_28"]=grouped.transform(lambda x: x.shift(1).rolling(28).mean())
    df["quantity_roll_std_7"]=grouped.transform(lambda x: x.shift(1).rolling(7).std())
    df["quantity_roll_std_28"]=grouped.transform(lambda x: x.shift(1).rolling(28).std())
    lag_cols = ["quantity_lag_1", "quantity_lag_7", "quantity_lag_14", "quantity_lag_28","quantity_roll_mean_7", "quantity_roll_mean_28", "quantity_roll_std_7", "quantity_roll_std_28"]
    df[lag_cols] = df[lag_cols].fillna(0)

    logger.info("Created lag features and rolling statistics for quantity_sold.")

    # drop unnecessary columns
    df.drop(columns=['revenue_egp', 'profit_egp',"cost_egp"],inplace=True)
    logger.info("Dropped unnecessary columns: revenue_egp, profit_egp, cost_egp.")

    upload_to_s3(df, new_file_path, bucket)
    logger.info(f"Uploaded engineered features to s3://{bucket}/{new_file_path}. Shape: {df.shape}")
    return {"bucket": bucket, "file_paths": new_file_path}

    