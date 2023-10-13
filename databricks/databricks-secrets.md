# Using secrets in Databricks

## 1. Install the Databricks CLI
Run the following command to install the Databricks CLI with cURL:

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh
```

More information [here](https://docs.databricks.com/en/dev-tools/cli/install.html)

## 2. Set up authentication for the Databricks CLI

### 2.1 Generate a personal access
[Guide](https://docs.databricks.com/en/dev-tools/auth.html#databricks-personal-access-tokens-for-workspace-users)
### 2.2 Create a configuration profile
On Linux or macOS:
Create a file in the home directory called `.databrickscfg` with the following contents:
```
[DEFAULT]
host  = <databricks host url>
token = <databricks personal access token>
```

## 3. Create secret scope
```bash
databricks secrets create-scope <scope name> --initial-manage-principal users
```

## 4. Create scecrets
```bash
databricks secrets put-secret <scope name> <key>
```

## 5. Use secrets
In Databricks:
```python
dbutils.secrets.get(scope = "<scope name>", key = <key>)
```
