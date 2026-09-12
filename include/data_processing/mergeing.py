from utils.minio_s3 import define_s3_hook
from include.logging.logger import get_logger
import os
import pandas as pd
import duckdb
logger =get_logger("mergeing")


def merge_data(data):
    s3_hook=define_s3_hook()
    bucket=data["bucket"]
    output_path=os.getenv("output_path_merge")
    con=duckdb.connect()

    con.execute("""
                INSTALL httpfs; LOAD httpfs;
            SET s3_endpoint='minio:9000';
            SET s3_access_key_id='minioadmin';
            SET s3_secret_access_key='minioadmin';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
            """)
    logger.info(f"setup duckdb connection to s3://{bucket}/{output_path}")
    
    con.execute(f"CREATE VIEW sales AS SELECT * FROM read_parquet('s3://{bucket}/raw_zone/sales/**/*.parquet');")
    
    con.execute(f"CREATE VIEW inventory AS SELECT product_id, store_id, date, inventory_level, reorder_point, days_of_supply FROM read_parquet('s3://{bucket}/raw_zone/inventory/**/*.parquet');")
    
    con.execute(f"CREATE VIEW promotions AS SELECT product_id,date, promotion_type, discount_percent FROM read_parquet('s3://{bucket}/raw_zone/promotions/**/*.parquet');")
    
    con.execute(f"CREATE VIEW store_events AS SELECT store_id, date, event_type, impact FROM read_parquet('s3://{bucket}/raw_zone/store_events/**/*.parquet');")
    
    con.execute(f"CREATE VIEW customer_traffic AS SELECT store_id, date, customer_traffic, weather_impact, is_holiday FROM read_parquet('s3://{bucket}/raw_zone/customer_traffic/**/*.parquet');")
    logger.info(f"Created views for sales, inventory, promotions, store_events, and customer_traffic in DuckDB")
            
    merge_query=f"""
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
            """
    logger.info(f"Executing merge query for s3://{bucket}/{output_path}")
    con.execute(merge_query)
    con.close()
    return {"bucket": bucket, "output_path": output_path}
    