import logging
from flood.api.glofas_fetcher import GloFASFetcher
from flood.api.config import GloFASAPIConfig

logging.basicConfig(level=logging.INFO)

class GloFASClient:
    def __init__(self, api_url, api_key):
        self.fetcher = GloFASFetcher(api_url, api_key)

    def fetch_grib_data(self, request_params, grib_output_path):
        """
        Fetch the GloFAS data in GRIB format and save to the given output path.
        """
        self.fetcher.fetch_grib(request_params, grib_output_path)
        
# Example usage
if __name__ == "__main__":
    client = GloFASClient(api_url="https://cds.climate.copernicus.eu/api/v2", 
                          api_key="<UID>:<APIKEY>")
    config = GloFASAPIConfig(
        year='2023',
        month='10',
        day='10',
        leadtime_hour='24',
        area=[17, -18, -6, 52]
    )
    request_params = config.to_dict()
    client.fetch_grib_data(request_params, "download.grib")
