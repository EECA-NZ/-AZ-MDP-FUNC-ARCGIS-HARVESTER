'''
Tests for arcgis_harvester function
'''

from unittest.mock import patch

import pytest

from arcgis_harvester import (
    __fetch_metadata_from_layer,
    __fetch_token,
    __upload_to_blob,
    __write_to_csv,
)


def test_fetch_token_success():
    '''Fetch token successfully'''
    with patch("requests.post") as mocked_post:
        mocked_post.return_value.status_code = 200
        mocked_post.return_value.json.return_value = {"token": "some-token"}
        token = __fetch_token()
        assert token == "some-token"


def test_fetch_token_failure():
    '''Fetch token unsuccessfully'''
    with patch("requests.post") as mocked_post:
        mocked_post.return_value.status_code = 200
        mocked_post.return_value.json.return_value = {"error": "some-error"}
        token = __fetch_token()
        assert token is None


def test_fetch_metadata_from_layer_success():
    '''Fetch metadata successfully'''
    token = "test-token"
    layer_url = "http://fake-url.com/layer"
    expected_metadata = {"name": "test-layer", "type": "Feature Layer"}

    with patch("requests.get") as mocked_get:
        mocked_get.return_value.status_code = 200
        mocked_get.return_value.json.return_value = expected_metadata
        metadata = __fetch_metadata_from_layer(layer_url, token)
        assert metadata == expected_metadata


def test_write_to_csv():
    '''Write to csv successfully'''
    data = [
        {
            "properties": {"prop1": "value1", "prop2": "value2"},
            "geometry": {"type": "Point", "coordinates": [125.6, 10.1]},
        }
    ]
    output = __write_to_csv(data)
    output.seek(0)
    lines = output.readlines()
    assert len(lines) > 1  # Header plus data line
    assert "prop1" in lines[0]
    assert "value1" in lines[1]


@pytest.fixture
def blob_service_client_mock():
    '''Mock the blob service client'''
    # Mock for Azure BlobServiceClient
    with patch("azure.storage.blob.BlobServiceClient") as mock:
        yield mock


def test_upload_to_blob(mocker):
    '''Upload to blob successfully'''
    # Arrange
    connection_string = "DefaultEndpointsProtocol=https;AccountName=..."
    container_name = "my-container"
    blob_path_prefix = "path/to/blob"
    blob_name = "test_blob.csv"
    blob_data = mocker.MagicMock(name="StringIO")
    blob_data.read.return_value = "blob content"

    # Mock the BlobServiceClient and its chain of calls
    mock_blob_service_client = mocker.patch("arcgis_harvester.BlobServiceClient")
    mock_blob_client = mocker.MagicMock()
    mock_blob_service_client.from_connection_string.return_value.get_blob_client.return_value = (
        mock_blob_client
    )

    # Act
    __upload_to_blob(
        connection_string, container_name, blob_path_prefix, blob_name, blob_data
    )

    # Assert
    # Verify BlobServiceClient called with the connection string
    mock_blob_service_client.from_connection_string.assert_called_once_with(
        connection_string
    )

    # Verify get_blob_client called with the correct container and blob path
    expected_blob_path = f"{blob_path_prefix}/{blob_name}"
    mock_blob_service_client.from_connection_string.return_value\
        .get_blob_client.assert_called_once_with(
            container=container_name, blob=expected_blob_path
    )

    # Verify blob_data.seek(0) was called to ensure the data is read from the beginning
    blob_data.seek.assert_called_once_with(0)

    # Verify upload_blob was called with the correct data
    mock_blob_client.upload_blob.assert_called_once_with(
        blob_data.read().encode("utf-8"), overwrite=True
    )
