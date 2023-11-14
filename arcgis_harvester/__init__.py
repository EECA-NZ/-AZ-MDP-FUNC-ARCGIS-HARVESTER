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


def __fetch_token():
    response = requests.post(AUTH_URL, data=AUTH_PARAMS, timeout=30)
    response.raise_for_status()
    token_data = response.json()
    if "token" not in token_data:
        logging.error("Failed to fetch token: %s", token_data.get("error", ""))
        return None

    return token_data["token"]


def __fetch_metadata_from_layer(layer_url, token):
    params = {"f": "json", "token": token}
    response = requests.get(layer_url, params=params, timeout=30)
    if response.status_code != 200:
        logging.error("Invalid response = %s", response.status_code)
        return []

    data = response.json()
    return data


def __fetch_data_from_layer(layer_url, token):
    query_url = layer_url + "/query"
    params = {**QUERY_PARAMS, "token": token}
    response = requests.get(query_url, params=params, timeout=30)
    if response.status_code != 200:
        logging.error("Invalid response = %s", response.status_code)
        return []

    data = response.json()
    if "features" not in data:
        logging.error("Error retrieving features: %s", data.get("error", ""))
        return []

    return data["features"]


def __write_to_csv(data):
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


def __fetch_metadata_from_blob(metadata_name):
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


def __upload_to_blob(
    connection_string, container_name, blob_path_prefix, blob_name, blob_data
):
    blob_file_path = blob_path_prefix + "/" + blob_name

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_file_path
    )

    # Convert the StringIO object to bytes and upload
    blob_data.seek(0)
    blob_client.upload_blob(blob_data.read().encode("utf-8"), overwrite=True)


def __dict_to_file(metadata_dict):
    metadata_str = json.dumps(metadata_dict)  # Convert dictionary to string
    return io.StringIO(metadata_str)  # Convert string to file-like object


def __get_nested_key(data, *keys):
    try:
        for key in keys:
            data = data[key]
        return data
    except KeyError as e:
        logging.error("Key error - %s - %s", str(data), str(e))
        return None
    except TypeError as e:
        logging.error("Type error - %s - %s", str(data), str(e))
        return None


def main(mytimer: func.TimerRequest) -> None:
    '''Main function'''
    utc_timestamp = (
        datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    )
    logging.info(
        "The timer is past due!"
        if mytimer.past_due
        else f"Python timer trigger function ran at {utc_timestamp}"
    )

    token = __fetch_token() if ARCGIS_PASSWORD and ARCGIS_USERNAME else None

    for layer in LAYERS_TO_IMPORT:
        logging.info("Processing layer: %s", layer)
        metadata = __fetch_metadata_from_layer(layer, token)
        csv_name = re.sub(r"[^\w]", "_", metadata["name"]) + ".csv"
        metadata_name = f"{csv_name}.json"
        blob_metadata = __fetch_metadata_from_blob(metadata_name)

        blob_edit_date = (
            __get_nested_key(blob_metadata, "editingInfo", "lastEditDate")
            if blob_metadata else None
        )
        edit_date = __get_nested_key(metadata, "editingInfo", "lastEditDate")

        if not metadata or metadata.get("type") != "Feature Layer":
            logging.warning(
                "Not a feature Layer." if metadata else "Metadata not found."
            )
            continue

        if edit_date and blob_edit_date and edit_date <= blob_edit_date:
            logging.info("No data updates since last time. Skipping.")
            continue

        data = __fetch_data_from_layer(layer, token)

        if not data:
            logging.error("Failed to fetch data for layer: %s", layer)
            continue

        csv_file, metadata_file = __write_to_csv(data), __dict_to_file(metadata)

        __upload_to_blob(
            AZURE_CONNECT_STR, CONTAINER_NAME, CSV_FILE_PATH_PREFIX, metadata_name, metadata_file,
        )
        __upload_to_blob(
            AZURE_CONNECT_STR, CONTAINER_NAME, CSV_FILE_PATH_PREFIX, csv_name, csv_file
        )

        logging.info(
            "Layer %s processed successfully.", CSV_FILE_PATH_PREFIX
        )
