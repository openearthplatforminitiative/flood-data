# Databricks

## Build and add package to Databricks

### 1. Building the Project
   
Before you can upload the package to Databricks, you need to build it. To do this, navigate to the root directory of the project and execute the following commands:

```
pip install setuptools wheel
python setup.py sdist bdist_wheel
```
This will generate a `.whl` (wheel) file in the `dist/` directory. This wheel file is what you'll upload to Databricks.

### 2. Uploading and Installing the Package to Databricks

1. Navigate to the Databricks workspace and select the target cluster.
2. Under the cluster details, click on the `Libraries` tab.
3. Click on `Install New` > `Upload`.
4. For `Library Type`, choose `Python Whl`.
5. Click on the upload option and select the `.whl` file you generated in the first step.
6. Confirm to complete the installation.
After following these steps, the package will be available for use in any notebook running on the selected cluster.
