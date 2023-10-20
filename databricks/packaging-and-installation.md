# Using the `flood` package in Databricks

## Building the Project and Installing the Package on Databricks
   
Fist, the `flood` needs to be built. To do this, navigate to the root directory of the project and execute the following commands:

```
pip install setuptools wheel
python3 setup.py sdist bdist_wheel
```
This will generate a `.whl` (wheel) file in the `dist/` directory. This wheel file is what you'll upload to Databricks.

Next, the package can be installed on the Databricks cluster

1. Navigate to the Databricks workspace and select the target cluster.
2. Under the cluster details, click on the `Libraries` tab.
3. Click on `Install New` > `Upload`.
4. For `Library Type`, choose `Python Whl`.
5. Click on the upload option and select the `.whl` file generated in the first step.
6. Confirm to complete the installation.
After following these steps, the package will be available for use in any notebook running on the selected cluster.

## Alternative solution

The `flood` package can be imported directly from the cloned repository without the need to package it. To achieve this, simply modify the Python path at runtime to include the directory that contains the `flood` package:
```
import sys
repo_path = '/Workspace/Repos/OpenEPI/flood-data'
sys.path.append(repo_path)
```
The repo path can also be found by running:
```
import json
import requests

ctx = json.loads(
  dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())

notebook_path = ctx['extraContext']['notebook_path']
repo_path = '/'.join(notebook_path.split('/')[:4])
```

Furthermore, if the `flood` package hasn't been installed with the packaged `.whl` file (which includes dependencies), the dependencies are installed using the `install_dependencies.sh` script (already configured as an init script for the cluster). This is mainly because installing the `ecmwflibs` package inside the cluster's `Libraries` configuration panel does not work (which is strange).