"""
Class that will provide code utilities to interact with GCP
"""

import os

import google.api_core.exceptions as gcp_exception
from google.cloud.storage.blob import Blob
from google.cloud import storage

class GCPStorageConnection:
    def __init__(self):
        self.storage_client = storage.Client()

    def list_objects(self, bucket: str = "", prefix: str = "", delimiter=None):
        try:
            blobs = self.storage_client.list_blobs(bucket, prefix=prefix, delimiter=delimiter)
        except gcp_exception.NotFound:
            return []

        blob_url_list = []
        for blob in blobs:
            blob_url_list.append(blob.name)
        return blob_url_list

    def get_gs_uri(self, bucket: str = "", blob_name: str = ""):
        return "gs://{}/{}".format(bucket, blob_name)
