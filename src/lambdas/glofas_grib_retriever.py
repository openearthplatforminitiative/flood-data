import boto3
from src.api.client import GloFASClient
from src.api.config import GloFASAPIConfig
from datetime import datetime, timedelta

def get_glofas_credentials():
    # Retrieve GloFAS API credentials from Secrets Manager
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId='GloFAS_API_Credentials')
    creds = eval(response['SecretString'])

    return creds

def lambda_handler(event, context):
    # Get credentials
    creds = get_glofas_credentials()

    # Retrieve the current date and subtract one day
    date_for_request = datetime.utcnow() - timedelta(days=1)
    year, month, day = date_for_request.strftime("%Y"), date_for_request.strftime("%m"), date_for_request.strftime("%d")

    # Retrieve leadtime_hour from event
    leadtime_hour = event.get('leadtime_hour', '24')  # default to 24 if not provided
    
    # Formulate the request_params using GloFASAPIConfig
    config = GloFASAPIConfig(
        year=year,
        month=month,
        day=day,
        leadtime_hour=leadtime_hour,
        area=event.get('area', [17, -18, -6, 52])
    )
    request_params = config.to_dict()

    # Initialize the GloFAS Client
    client = GloFASClient(api_url=creds['url'], api_key=creds['key'])
    
    # Construct unique file paths based on the date hierarchy and leadtime_hour
    grib_output_path = f"s3://your-bucket-name/staging/grib/{year}/{month}/{day}/forecast_leadtime_{leadtime_hour}.grib"
    
    # Fetch the data and save it as GRIB in the S3 staging area
    client.fetch_grib_data(request_params, grib_output_path)

    return {
        'statusCode': 200,
        'body': f"GRIB data fetched and saved for {year}-{month}-{day} and leadtime_hour: {leadtime_hour}"
    }
