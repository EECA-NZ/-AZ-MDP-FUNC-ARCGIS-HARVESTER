# AZ-MDP-FUNC-ARCGIS-HARVESTER

Azure function to harvest data from an ArcGIS API

We use the Azure Functions extension in Visual Studio Code to write Python code that can be integrated into the pipeline.

The python code is tested locally before deploying it to the environment of Azure Functions.

## Prerequisites

-  An Azure Key Vault with ArcGIS credentials stored as `arcgis_username` and `arcgis_password`.
-  An Azure Function App `$FUNCTION_APP` already configured, with a setting called `KeyVaultURL`.

## To see the function in Azure Portal:

-  Open Azure Portal
-  Click on the icon Resource groups
-  Select your resource group
-  Select your function app
-  On the side bar select Functions
-  Select the function

## To test the function we use VS Code and Postman using the following steps:

1. Install the Azure Functions extension in Visual Studio Code
2. Clone (or pull) the repo from GitHub
3. Open the repo folder in Visual Studio Code

## To sync down app settings from Azure:

```bash
func azure functionapp fetch-app-settings $ARCGIS_HARVESTER_FUNCTION_APP
```

## If needed, add ArcGIS credentials to the Azure Vault

```bash
export KEY_VAULT_NAME=$(jq -r .Values.KeyVaultURL local.settings.json | awk -F[/:] '{print $4}' | awk -F[.:] '{print $1}')
az login
az keyvault secret set --vault-name $KEY_VAULT_NAME --name "arcgis-username" --value $ARCGIS_USERNAME
az keyvault secret set --vault-name $KEY_VAULT_NAME --name "arcgis-password" --value $ARCGIS_PASSWORD
```

## To run the function locally in the VS Code terminal:

The currently deployed function apps use python 3.10.0, so it is recommended to use this version.

### Create a virtual environment

```bash
python3 -m venv .venv
```

### Activate the virtual environment and start the function

```bash
. .venv/bin/activate
pip install -r requirements.txt
func start
```

#### In Windows to activaste the virtual environment

```bash
.venv/Scripts/Activate.ps1
```
