[![Pylint](https://github.com/EECA-NZ/AZ-MDP-FUNC-ARCGIS-HARVESTER/actions/workflows/pylint.yml/badge.svg)](https://github.com/EECA-NZ/AZ-MDP-FUNC-ARCGIS-HARVESTER/actions/workflows/pylint.yml) [![Deploy to dev](https://github.com/EECA-NZ/AZ-MDP-FUNC-ARCGIS-HARVESTER/actions/workflows/deploy-to-dev.yml/badge.svg)](https://github.com/EECA-NZ/AZ-MDP-FUNC-ARCGIS-HARVESTER/actions/workflows/deploy-to-dev.yml)

# AZ-MDP-FUNC-ARCGIS-HARVESTER

Azure function to harvest data from an ArcGIS API and save as CSV (with geometries as GeoJSON) in Azure Blob Storage

We use the Azure Functions extension in Visual Studio Code to write Python code that can be integrated into the pipeline.

The python code is tested locally before deploying it to the environment of Azure Functions.

## Pre-requisites

Create an Azure function app with the required settings:

### Azure Storage connection string
[How to configure a connection string](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string#configure-a-connection-string-for-an-azure-storage-account)
- `StorageAccountConnectionString`

### ArcGIS user and password
If required, otherwise can be blank.
- `ArcGISUsername` 
- `ArcGISPassword`

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
Assumes all the required settings have been added.

```bash
export ARCGIS_HARVESTER_FUNCTION_APP=<name-of-your-function-app>
func azure functionapp fetch-app-settings $ARCGIS_HARVESTER_FUNCTION_APP
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
pip install -r requirements-dev.txt
func start
```

#### In Windows to activate the virtual environment

```bash
.venv/Scripts/Activate.ps1
```

## To run the unit tests

```bash
pytest
```
