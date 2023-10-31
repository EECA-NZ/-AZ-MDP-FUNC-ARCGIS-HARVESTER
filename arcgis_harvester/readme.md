# Azure Function: ArcGIS Data Harvester (TimerTrigger - Python)

The `ArcGIS Data Harvester` is a serverless Azure function which periodically harvests data from an ArcGIS Online Server and stores it in in Azure Blob Storage. This function is executed on a schedule as specified by a cron expression.

## Overview

This function fetches and processes data the ArcGIS Online REST API. All layers are fetched, attribute data converted to strings, with the geometry objects converted to Well Known Text (WKT) strings.

The data is then saved to Blob Storage as CSV files for ingestion into our SQL database.

## How it works

The function uses a `TimerTrigger` to run at a set schedule. This is defined by a [NCrontab expression](https://github.com/atifaziz/NCrontab) that specifies the timing for function execution.

For example, a cron expression like `0 0 7 * * *` means: "Run this function at 7 AM everyday". You can test your NCrontab exoression [here](https://ncrontab.swimburger.net/).

## Set Up and Execution

1. Make sure you have the necessary environment variables set up in your Azure Function. These include your Blob Storage connection strings, database connection details, and other important configuration values.

2. Deploy your Azure Function.

3. Once deployed, the function will run automatically at the specified schedule. You can also trigger it manually from the Azure portal if needed.
