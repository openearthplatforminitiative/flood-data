import xarray as xr
import logging
from flood.etl.utils import determine_engine

logging.basicConfig(level=logging.INFO)

class RasterConverter:

    def file_to_parquet(self, input_path, output_path, read_engine=None, write_engine='pyarrow', compression='snappy', cols_to_drop=None, drop_na_subset=None, drop_index=False, save_index=None):
        """
        Convert a raster (GRIB or NetCDF) file to Parquet format.
        
        :param input_path: Path to the raster file.
        :param output_path: Path to save the resulting Parquet file.
        :param read_engine: The engine to use for reading the raster file.
        :param write_engine: The engine to use for writing the Parquet file.
        :param compression: The compression algorithm to use for writing the Parquet file.
        :param cols_to_drop: List of columns to drop from the resulting dataframe.
        :param drop_na_subset: List of columns to use for dropping rows with NA values.
        :param drop_index: Whether to drop the index column when resetting the index.
        :param save_index: Whether to save the index column.
        """
        try:
            # Determine the read engine if not specified
            if read_engine is None:
                read_engine = determine_engine(input_path)

            # Step 1: Read the raster file into an xarray Dataset
            ds = xr.open_dataset(input_path, engine=read_engine)

            # Step 2: Convert the xarray Dataset to a Pandas DataFrame
            df = ds.to_dataframe()

            # Drop unwanted columns
            if cols_to_drop is not None:
                for col in cols_to_drop:
                    if col in df.columns:
                        df = df.drop(columns=col)

            # Drop rows with NA values
            if drop_na_subset is not None:
                df = df.dropna(subset=drop_na_subset)

            df = df.reset_index(drop=drop_index)

            # Save the parquet file using dataframe_to_parquet
            # self.dataframe_to_parquet(df, output_path, write_engine=write_engine, compression=compression, save_index=save_index)
                    
            # Step 3: Convert the Pandas DataFrame to a Parquet file using pyarrow and snappy compression
            df.to_parquet(output_path, engine=write_engine, compression=compression, index=save_index)

            logging.info(f"Converted {input_path} to {output_path} successfully!")

        except Exception as e:
            logging.error(f"Error during conversion of {input_path} to {output_path}: {e}")

    def dataset_to_dataframe(self, ds, cols_to_drop=None, drop_na_subset=None, drop_index=False):

        """
        Convert a raster in xarray.dataset format to a pandas dataframe.
        
        :param ds: The xarray dataset.
        :param cols_to_drop: List of columns to drop from the resulting dataframe.
        :param drop_na_subset: List of columns to use for dropping rows with NA values.
        :param drop_index: Whether to drop the index column when resetting the index.
        """
        try:
            df = ds.to_dataframe()

            # Drop unwanted columns
            if cols_to_drop is not None:
                for col in cols_to_drop:
                    if col in df.columns:
                        df = df.drop(columns=col)

            # Drop rows with NA values
            if drop_na_subset is not None:
                df = df.dropna(subset=drop_na_subset)

            df = df.reset_index(drop=drop_index)

            logging.info(f"Converted xarray dataset to pandas dataframe successfully!")

            return df

        except Exception as e:
            logging.error(f"Error during conversion of xarray dataset to pandas dataframe: {e}")
            return None
        
    def dataframe_to_parquet(self, df, output_path, write_engine='pyarrow', compression='snappy', save_index=None):
        """
        Save a pandas dataframe in Parquet format.
        
        :param df: The pandas dataframe.
        :param output_path: Path to save the resulting Parquet file.
        :param write_engine: The engine to use for writing the Parquet file.
        :param compression: The compression algorithm to use for writing the Parquet file.
        :param save_index: Whether to save the index column.
        """
        try:
            df.to_parquet(output_path, engine=write_engine, compression=compression, index=save_index)

            logging.info(f"Converted pandas dataframe to {output_path} successfully!")

        except Exception as e:
            logging.error(f"Error during conversion of pandas dataframe to {output_path}: {e}")

# Example usage
if __name__ == "__main__":
    converter = RasterConverter()
    converter.file_to_parquet('sample_input.grib', 'sample_output.parquet', cols_to_drop=['surface'])
