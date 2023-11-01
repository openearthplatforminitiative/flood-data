# flood-data
Data pipeline for flood data

## Project Structure

The directory structure is the following:

```
flood-data/
│
├── README.md                           # Project description
├── LICENSE                             # Project license
├── .gitignore                          # Files and directories to be ignored by Git
├── requirements.txt                    # Required Python packages for this project
├── setup.py                            # For packaging the project
│
├── flood/                              # flood package
│   ├── __init__.py
│   ├── api/                            # Code related to the GloFAS API
│   │   ├── __init__.py
│   │   ├── client.py                   # Logic to interact with the GloFAS API
│   │   ├── config.py                   # API request configurations
│   │   └── glofas_fetcher.py           # Lambda function to fetch the GRIB data
│   │
│   ├── etl/                            # Extract, Transform, Load operations
│   │   ├── __init__.py
│   │   ├── raster_to_parquet.py        # Logic to convert raster files to Parquet
│   │   ├── filter_by_upstream.py       # Logic to filter discharge data by upstream area
│   │   └── utils.py                    # ETL utils/helpers
│   │
│   ├── spark/                          # Code related to Spark
│   │   ├── __init__.py
│   │   └── transforms.py               # Spark transforms
│   │
│   └── utils/                          # Miscellaneous utils/helpers
│       ├── __init__.py
│       └── config.py                   # Config helpers
│   
├── test/                               # Unit tests
│   ├── __init__.py
│   ├── data/                           # Synthetic data generation for tests
│   │   ├── __init__.py
│   │   └── data_generation.py          # Script for synthetic data generation
│   ├── test_upstream_filtering.py      # Test the upsatream filtering mechanism
│   ├── test_restrict_dataset.py        # Test the dataset restriction mechanism
│   └── test_spark_transforms.py        # Test Spark transforms
│
└── databricks/                         # Databricks specific scripts or notebooks
    ├── scripts/                        # Init scripts for cluster
    ├── guides/                         # Helpful guides for using code in Databricks
    ├── config.json                     # Configuration parameters and constants for all notebooks
    ├── glofas-API-query.py             # Notebook for fetching and downloading GloFAS data
    ├── glofas-transform.py             # Notebook for converting raster GloFAS data to Parquet
    ├── glofas-forecast-computation.py  # Notebook for computing summary and detailed forecasts
    └── ...
```