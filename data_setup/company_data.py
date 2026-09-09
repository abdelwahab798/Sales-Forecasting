import os
from datetime import datetime
from boto3 import client
from include.utils.data_genrator import RealisticSalesDataGenerator

s3_client = client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

BUCKET_NAME ="company-raw-data"

def init_cloud_storage():
    print("Checking MinIO Cloud Storage...")
    existing_buckets = [b['Name'] for b in s3_client.list_buckets().get('Buckets', [])]
    if BUCKET_NAME not in existing_buckets:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        print(f"Created Bucket: '{BUCKET_NAME}'")
    else:
        print(f"Bucket '{BUCKET_NAME}' is ready.")

def generate_and_upload_data():
    temp_dir = "/tmp/local_gen_data"
    
    print("Step 1: Generating Raw Company Data...")
    generator = RealisticSalesDataGenerator(
        start_date=datetime(2023, 1, 1), 
        end_date=datetime(2024, 12, 31)
    )
    generated_files = generator.generate_sales_data(output_dir=temp_dir)
    
    print("\nStep 2: Uploading Data to Company Cloud (MinIO S3)...")
    
    for category, file_paths in generated_files.items():
        print(f"⬆Uploading {len(file_paths)} files for category: '{category}'...")
        for file_path in file_paths:
            file_name = os.path.basename(file_path)
            s3_key = f"raw_zone/{category}/{file_name}"
            s3_client.upload_file(file_path, BUCKET_NAME, s3_key)
            
    print(f"\nSUCCESS: All company data is now live on Cloud Bucket: '{BUCKET_NAME}'!")

if __name__ == "__main__":
    init_cloud_storage()
    generate_and_upload_data()