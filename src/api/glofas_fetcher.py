import logging
import cdsapi

logging.basicConfig(level=logging.INFO)

class GloFASFetcher:
    def __init__(self, api_url, api_key):
        self.client = cdsapi.Client(url=api_url, key=api_key)

    def fetch_grib(self, request_params, grib_output_path):
        """
        Fetch data from GloFAS and save it as a GRIB.
        """
        try:
            logging.info("Fetching GRIB data from GloFAS...")
            self.client.retrieve(
                'cems-glofas-forecast',
                request_params,
                grib_output_path
            )
            logging.info(f"GRIB data saved to {grib_output_path}")
        except Exception as e:
            logging.error(f"Error fetching GRIB data: {e}")


# Example usage
if __name__ == "__main__":
    # Fetch GloFAS data, save as GRIB, then convert to Parquet and save again.
    # This example usage can be adapted to your AWS Lambda handler or other deployment method.
    fetcher = GloFASFetcher(api_url="https://cds.climate.copernicus.eu/api/v2", 
                            api_key="YOUR_API_KEY")
    request_params = {
        'system_version': 'operational',
        'hydrological_model': 'lisflood',
        'product_type': 'ensemble_perturbed_forecasts',
        'variable': 'river_discharge_in_the_last_24_hours',
        'year': '2023',
        'month': '10',
        'day': '10',
        'leadtime_hour': [
            '24', '48',
        ],
        'format': 'grib',
        'area': [
            17, -18, -6,
            52,
        ],
    }
    fetcher.fetch_grib(request_params, "download.grib", "download.parquet")
