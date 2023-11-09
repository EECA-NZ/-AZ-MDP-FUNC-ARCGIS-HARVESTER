"""
This function runs on a timer and harvests data from an ArcGIS server
"""

import csv
import datetime
import io
import json
import logging
import os
import re

import azure.functions as func
import requests
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from shapely.geometry import mapping, shape

from arcgis_harvester.constants import *

# Get variables from Azure Function configuration
AZURE_CONNECT_STR = os.getenv("StorageAccountConnectionString")
ARCGIS_USERNAME = os.getenv("ArcGISUsername")
ARCGIS_PASSWORD = os.getenv("ArcGISPassword")

# Constants for ArcGIS API
QUERY_PARAMS = {
    "f": "geojson",
    "where": "1=1",
    "outFields": "*",
    "returnGeometry": "true",
}
AUTH_URL = "https://www.arcgis.com/sharing/rest/generateToken"
AUTH_PARAMS = {
    "username": ARCGIS_USERNAME,
    "password": ARCGIS_PASSWORD,
    "f": "json",
    "referer": "https://www.arcgis.com",
    "expiration": 60,  # Token expiration time in minutes
}


def fetch_token():
    response = requests.post(AUTH_URL, data=AUTH_PARAMS, timeout=30)
    response.raise_for_status()
    token_data = response.json()
    if "token" in token_data:
        return token_data["token"]
    else:
        logging.error("Failed to fetch token: %s", token_data.get("error", ""))
        return None


def fetch_metadata_from_layer(layer_url, token):
    params = {"f": "json", "token": token}
    response = requests.get(layer_url, params=params, timeout=30)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        logging.error("Invalid response = %s", response.status_code)
        return []


def fetch_data_from_layer(layer_url, token):
    query_url = layer_url + "/query"
    params = {**QUERY_PARAMS, "token": token}
    response = requests.get(query_url, params=params, timeout=30)
    if response.status_code == 200:
        data = response.json()
        if "features" in data:
            return data["features"]
        else:
            logging.error("Error retrieving features: %s", data.get("error", ""))
            return []
    else:
        logging.error("Invalid response = %s", response.status_code)
        return []


def write_to_csv(data):
    output = io.StringIO()
    attributes = data[0]["properties"].keys()
    fieldnames = list(attributes) + ["geometry"]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()

    for item in data:
        row = item["properties"]
        # convert geometry to well known text
        if item["geometry"]:
            geometry = shape(item["geometry"]).wkt
            row["geometry"] = geometry
        writer.writerow(row)

    return output


def fetch_metadata_from_blob(metadata_name):
    try:
        blob_file_path = CSV_FILE_PATH_PREFIX + "/" + metadata_name
        blob_service_client = BlobServiceClient.from_connection_string(
            AZURE_CONNECT_STR
        )
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME, blob=blob_file_path
        )

        blob_data = blob_client.download_blob()
        content = blob_data.readall()

        content_str = content.decode("utf-8")
        content_json = json.loads(content_str)

        return content_json

    except ResourceNotFoundError:
        logging.info("Metadata blob not found.")
        return []


def upload_to_blob(connection_string, container_name, blob_path_prefix, blob_name, blob_data):
    blob_file_path = blob_path_prefix + "/" + blob_name

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_file_path
    )

    # Convert the StringIO object to bytes and upload
    blob_data.seek(0)
    blob_client.upload_blob(blob_data.read().encode("utf-8"), overwrite=True)


def dict_to_file(metadata_dict):
    metadata_str = json.dumps(metadata_dict)  # Convert dictionary to string
    return io.StringIO(metadata_str)  # Convert string to file-like object


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = (
        datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    )

    if mytimer.past_due:
        logging.info("The timer is past due!")

    logging.info("Python timer trigger function ran at %s", utc_timestamp)

    if ARCGIS_PASSWORD and ARCGIS_USERNAME:
        token = fetch_token()
    else:
        token = None

    for layer in LAYERS_TO_IMPORT:
        logging.info("Processing layer: %s", layer)

        edit_date = None
        blob_edit_date = None

        metadata = fetch_metadata_from_layer(layer, token)

        csv_name = re.sub(r"[^\w]", "_", metadata["name"]) + ".csv"
        metadata_name = csv_name + ".json"

        blob_metadata = fetch_metadata_from_blob(metadata_name)

        if blob_metadata:
            # Retrieve the last edit date from already imported metadata
            try:
                blob_edit_date = blob_metadata["editingInfo"]["lastEditDate"]
            except KeyError:
                logging.error(
                    "The nested key 'lastEditDate' does not exist in %s.", metadata_name
                )
        else:
            logging.info(
                "No existing metadata found in %s. Will try and import anyway.",
                CSV_FILE_PATH_PREFIX,
            )

        if metadata:
            # Make sure it's a feature layer
            try:
                if metadata["type"] != "Feature Layer":
                    logging.warning("Skipping non Feature Layer")
                    continue
            except KeyError:
                logging.errror(
                    "The key 'type' does not exist in metadata %s.", metadata_name
                )
                continue

            # Retrieve the last edit date from the layer
            try:
                edit_date = metadata["editingInfo"]["lastEditDate"]
            except KeyError:
                logging.error(
                    "The nested key 'lastEditDate' does not exist in %s.", layer
                )
        else:
            logging.error("Metadata not found for layer %s.", layer)
            continue

        # Compare the last edit dates
        if edit_date and blob_edit_date:
            try:
                if edit_date <= blob_edit_date:
                    logging.info("No data updates since last time. Skipping.")
                    continue
            except TypeError:
                logging.warning(
                    "Type error, could not compare types %s and %s.",
                    edit_date,
                    blob_edit_date,
                )

        # Retrieve data and write to blob storage
        data = fetch_data_from_layer(layer, token)
        if data:
            csv_file = write_to_csv(data)
            metadata_file = dict_to_file(metadata)
            
            upload_to_blob(AZURE_CONNECT_STR, CONTAINER_NAME, CSV_FILE_PATH_PREFIX, metadata_name, metadata_file)
            upload_to_blob(AZURE_CONNECT_STR, CONTAINER_NAME, CSV_FILE_PATH_PREFIX, csv_name, csv_file)

            logging.info(
                "Metadata fetched and saved to %s successfully.", metadata_name
            )
            logging.info("Data fetched and saved to %s successfully.", csv_name)
        else:
            logging.error("Failed to fetch data for layer: %s", layer)
