import boto3
from botocore.exceptions import ClientError 

def check_bucket(bucket_name):

    s3 = boto3.client("s3")

    try:
        s3.head_bucket(Bucket=bucket_name)
        print("Yes, bucket exists")

    except ClientError:
        print("No, bucket not exist")

check_bucket("us-flight-project")