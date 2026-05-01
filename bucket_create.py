import boto3  
from botocore.exceptions import ClientError  # AWS errors handle 
import re
from pathlib import Path


# Bucket create

def create_bucket_if_not_exists(bucket_name, region="ap-south-1"):
    """
    Ye function check karta hai ki S3 bucket exist karti hai ya nahi.
    Agar nahi hai, to nayi bucket create kar deta hai.
    
    bucket_name : str : S3 bucket ka naam
    region : str : AWS region (default = Mumbai 'ap-south-1')
    """
    # S3 client create karna
    s3 = boto3.client("s3", region_name=region)

    try:
        # check buckket exit
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket pehle se exist karti hai: {bucket_name}")

    except ClientError:
        # if bucket not exit create new
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region}
        )
        print(f"Nayi bucket create ho gayi: {bucket_name}")


# Function 2: Upload CSVs with year/month partition

def upload_csvs_to_s3_partitioned(local_folder, bucket_name):
    """
    Local folder se CSV files ko S3 me upload karta hai,
    year/month partition ke structure ke saath.
    
    local_folder : str : Local folder path jahan CSV files hain
    bucket_name : str : Target S3 bucket
    """
    region = "ap-south-1"
    s3 = boto3.client("s3", region_name=region)

    # Local folder me jitni bhi CSV files hain unpe loop
    for file_path in Path(local_folder).glob("*.csv"):
        # Filename se year aur month extract karna
        match = re.search(r"1987_present_(\d{4})_(\d{1,2})\.csv$", file_path.name)
        if not match:
            print(f"Skip ho gayi file (pattern mismatch): {file_path.name}")
            continue

        year = match.group(1)
        month = match.group(2).zfill(2)  # 01, 02, ... 12

        # S3 key with partitioning
        s3_key = f"bts_flight_data/flight/year={year}/month={month}/{file_path.name}"

        # Upload CSV to S3
        s3.upload_file(str(file_path), bucket_name, s3_key)
        print(f" file uploded -> s3://{bucket_name}/{s3_key}")

# Main Script
if __name__ == "__main__":
    # Bucket name
    bucket_name = "us-flight-project"

    # 1️⃣ Check/create bucket
    create_bucket_if_not_exists(bucket_name)

    # 2️⃣ Upload CSV files from local folder
    upload_csvs_to_s3_partitioned(
        local_folder="./downloaded_files",
        bucket_name=bucket_name
    )