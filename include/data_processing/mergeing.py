from include.logger.main_logger import get_logger
import os
import pandas as pd
import duckdb
logger =get_logger("mergeing")


def merge_data(data):
    bucket=data["bucket"]
    output_path=os.getenv("OUTPUT_PATH_MERGE")
    endopoint="minio:9000"
    secrets="minioadmin"
    con=duckdb.connect()

    con.execute(f"""
                INSTALL httpfs; LOAD httpfs;
            SET s3_endpoint='{endopoint}';
            SET s3_access_key_id='{secrets}';
            SET s3_secret_access_key='{secrets}';
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
           COPY (
    WITH distinct_products AS (
        -- استخراج قائمة المنتجات الثابتة بأسعارها وفئاتها
        SELECT DISTINCT product_id, category, unit_price_egp 
        FROM sales
    ),
    closure_sales AS (
        -- توليد صفوف مبيعات بصفر لكل المنتجات في أيام إغلاق الفروع
        SELECT 
            e.date,
            e.store_id,
            p.product_id,
            p.category,
            0 AS quantity_sold,
            p.unit_price_egp,
            0.0 AS discount_percent,
            0.0 AS revenue_egp,
            0.0 AS cost_egp,
            0.0 AS profit_egp
        FROM store_events e
        CROSS JOIN distinct_products p
        WHERE NOT EXISTS (
            SELECT 1 FROM sales s 
            WHERE s.store_id = e.store_id AND s.date = e.date AND s.product_id = p.product_id
        )
    ),
    complete_sales AS (
        SELECT 
            date, store_id, product_id, category, quantity_sold, 
            unit_price_egp, discount_percent, revenue_egp, cost_egp, profit_egp 
        FROM sales
        UNION ALL
        SELECT * FROM closure_sales
    )
    SELECT 
        s.*,
        i.inventory_level, i.reorder_point, i.days_of_supply,
        COALESCE(p.promotion_type, 'no_promotion') AS promotion_type,
        COALESCE(e.event_type, 'normal_day') AS event_type,
        COALESCE(e.impact, 0.0) AS impact,
        c.customer_traffic, c.weather_impact, c.is_holiday
    FROM complete_sales s
    LEFT JOIN inventory i
        ON s.product_id = i.product_id AND s.store_id = i.store_id AND s.date = i.date
    LEFT JOIN promotions p
        ON s.product_id = p.product_id AND s.date = p.date
    LEFT JOIN store_events e
        ON s.store_id = e.store_id AND s.date = e.date
    LEFT JOIN customer_traffic c
        ON s.store_id = c.store_id AND s.date = c.date
    ORDER BY s.store_id, s.product_id, s.date
) TO 's3://{bucket}/{output_path}' (FORMAT PARQUET);
            """
    logger.info(f"Executing merge query for s3://{bucket}/{output_path}")
    con.execute(merge_query)
    con.close()
    return {"bucket": bucket, "file_paths": output_path}
    