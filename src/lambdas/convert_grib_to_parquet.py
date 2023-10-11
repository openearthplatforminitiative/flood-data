import boto3
from src.etl.grib_to_parquet import GribToParquetConverter

def lambda_handler(event, context):
    # Instantiate the converter
    converter = GribToParquetConverter()
    
    # Retrieve the S3 key of the GRIB file from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Download the file to Lambda temp space
    s3_client = boto3.client('s3')
    s3_client.download_file(bucket, key, "/tmp/temp.grib")
    
    # Convert it to Parquet using the provided converter class
    parquet_output_path = "/tmp/" + key.split('/')[-1].replace(".grib", ".parquet")
    converter.convert("/tmp/temp.grib", parquet_output_path)
    
    # Construct the S3 parquet output path
    s3_parquet_output_path = key.replace("staging/", "parquet/").replace(".grib", ".parquet")
    
    # Upload Parquet to S3
    with open(parquet_output_path, 'rb') as f:
        s3_client.upload_fileobj(f, bucket, s3_parquet_output_path)
    
    # Delete the original GRIB file from staging area
    s3_client.delete_object(Bucket=bucket, Key=key)

    return {
        'statusCode': 200,
        'body': f"Converted {key} to {s3_parquet_output_path} and removed the original GRIB file."
    }
