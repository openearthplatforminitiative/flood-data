# Project Structure

The directory structure of the flood-data repository could look like this:

```
flood-data/
│
├── README.md                           # Project description, setup instructions, etc.
│
├── .gitignore                          # Files and directories to be ignored by Git
│
├── requirements.txt                    # Required Python packages for this project
│
├── setup.py                            # If you plan to package the project 
│
├── flood_processing/                   # Source code
│   ├── __init__.py
│   ├── api/                            # Code related to the GloFAS API
│   │   ├── __init__.py
│   │   ├── client.py                   # Logic to interact with the GloFAS API
│   │   ├── config.py                   # API request configurations
│   │   ├── glofas_fetcher.py           # Lambda function to fetch the GRIB data
│   │   └── ...
│   │
│   ├── etl/                            # Extract, Transform, Load operations
│   │   ├── __init__.py
│   │   ├── raster_to_parquet.py        # Logic to convert raster files to Parquet
│   │   └── ...
│   │
│   ├── utils/                          # Utility functions and classes
│   │   ├── __init__.py
│   │   ├── logger.py                   # Logging utility
│   │   └── ...
│   │
│   └── ...
│
├── test/                              # Unit tests
│   ├── __init__.py
│   ├── data/                          # Dummy data generation for tests
│   │   ├── __init__.py
│   │   ├── data_generation.py
│   │   └── ...
│   ├── test_upstream_filtering.py     
│   └── ...
│
├── databricks/                          # Databricks specific scripts or notebooks
│   └── ...
│
└── docs/                                # Documentation (could be auto-generated or manual)
    ├── index.md
    └── ...
```