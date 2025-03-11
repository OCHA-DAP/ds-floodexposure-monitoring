from typing import Literal

import ocha_stratus as stratus
from azure.storage.blob import ContentSettings


def load_blob_data(
    blob_name,
    stage: Literal["prod", "dev"] = "dev",
    container_name: str = "projects",
):
    container_client = stratus.get_container_client(
        stage=stage, container_name=container_name
    )
    blob_client = container_client.get_blob_client(blob_name)
    data = blob_client.download_blob().readall()
    return data


def upload_blob_data(
    blob_name,
    data,
    stage: Literal["prod", "dev"] = "dev",
    container_name: str = "projects",
    content_type: str = None,
):
    container_client = stratus.get_container_client(
        stage=stage, container_name=container_name, write=True
    )

    if content_type is None:
        content_settings = ContentSettings(
            content_type="application/octet-stream"
        )
    else:
        content_settings = ContentSettings(content_type=content_type)

    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(
        data, overwrite=True, content_settings=content_settings
    )
