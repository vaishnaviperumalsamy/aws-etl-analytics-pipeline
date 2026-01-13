# Standard library imports
import json                     
import io                       
from datetime import datetime  
# AWS SDK
import boto3                    # Used to interact with AWS services (S3, Glue)
import pandas as pd         


def flatten(data):
    
    orders_data = []
    # Loop through each order
    for order in data:
        # Loop through products inside each order
        for product in order['products']:
            # Create a flat record combining order, customer, and product details
            row_orders = {
                "order_id": order["order_id"],
                "order_date": order["order_date"],
                "total_amount": order["total_amount"],
                "customer_id": order["customer"]["customer_id"],
                "customer_name": order["customer"]["name"],
                "email": order["customer"]["email"],
                "address": order["customer"]["address"],
                "product_id": product["product_id"],
                "product_name": product["name"],
                "category": product["category"],
                "price": product["price"],
                "quantity": product["quantity"]
            }

            # Append flattened record to list
            orders_data.append(row_orders)

    # Convert list of dictionaries into a Pandas DataFrame
    df_orders = pd.DataFrame(orders_data)
    return df_orders


def lambda_handler(event, context):
    """
    AWS Lambda entry point.
    Triggered automatically when a JSON file is uploaded to S3.
    """

    # Extract S3 bucket name and object key from event metadata
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Initialize S3 client
    s3 = boto3.client('s3')

    # Fetch uploaded JSON file from S3
    response = s3.get_object(Bucket=bucket_name, Key=key)

    # Read and parse JSON content
    content = response['Body'].read().decode('utf-8')
    data = json.loads(content)

    # Flatten nested JSON into a DataFrame
    df = flatten(data)

    # Create an in-memory binary buffer (avoids writing to disk)
    parquet_buffer = io.BytesIO()

    # Convert DataFrame to Parquet format
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')

    # Generate timestamp for unique file naming
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")

    # Define destination path for Parquet file in S3
    key_staging = f'orders_parquet_datalake/orders_ETL_{timestamp}.parquet'

    # Upload Parquet file to S3 data lake location
    s3.put_object(
        Bucket=bucket_name,
        Key=key_staging,
        Body=parquet_buffer.getvalue()
    )

    # Initialize AWS Glue client
    glue = boto3.client('glue')

    # Trigger Glue crawler to update table schema
    crawler_name = 'etl_pipeline_1'
    response = glue.start_crawler(Name=crawler_name)

    # NOTE:
    # output_key is intended to reference the generated Parquet file path.
    # In this implementation, key_staging represents that path.

    return {
        "statusCode": 200,
        "message": "ETL job completed successfully",
        "output_file": output_key
    }

