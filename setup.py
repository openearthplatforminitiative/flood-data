from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="flood",
    version="0.1",  # start with a small version number
    description="A Python package to process flood data.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Aleksander Stangeland, Gil Tinde",
    author_email="aleksander.stangeland@knowit.no, gil.tinde@knowit.no",
    packages=find_packages(),  # Automatically discover and include all packages in the package directory
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    install_requires=[  # specify your dependencies here
        'cdsapi',
        'xarray',
        'pandas',
        'pyarrow',
        'netcdf4',
        'cfgrib',
        'numpy'
    ],
    python_requires='>=3.8',
)
