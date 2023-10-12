import xarray as xr
import logging

logging.basicConfig(level=logging.INFO)

class GribToParquetConverter:
    def __init__(self):
        pass

    def convert(self, input_path, output_path, read_engine='cfgrib', write_engine='pyarrow', compression='snappy', cols_to_drop=['surface']):
        """
        Convert a GRIB file to Parquet format.
        
        :param input_path: Path to the GRIB file.
        :param output_path: Path to save the resulting Parquet file.
        """
        try:
            # Step 1: Read the GRIB file into an xarray Dataset
            ds = xr.open_dataset(input_path, engine=read_engine)

            # Step 2: Convert the xarray Dataset to a Pandas DataFrame
            df = ds.to_dataframe().reset_index()

            # Drop unwanted columns
            for col in cols_to_drop:
                if col in df.columns:
                    df = df.drop(columns=col)

            # Step 3: Convert the Pandas DataFrame to a Parquet file using pyarrow and snappy compression
            df.to_parquet(output_path, engine=write_engine, compression=compression)

            logging.info(f"Converted {input_path} to {output_path} successfully!")

        except Exception as e:
            logging.error(f"Error during conversion of {input_path} to {output_path}: {e}")


# This can be used for testing or standalone conversions if needed
if __name__ == "__main__":
    converter = GribToParquetConverter()
    converter.convert('sample_input.grib', 'sample_output.parquet')
