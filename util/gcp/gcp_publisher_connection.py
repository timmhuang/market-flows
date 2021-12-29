"""
This is the wrapper class for connecting to GCP's PublisherClient API

Created By Kenneth Nursalim on 12/13/2020
"""

import base64
import json
import os

import logging

from google.cloud import pubsub_v1

class GCPPublisherConnection:
    def __init__(self, project_id: str, topic_id: str):
        self.client = pubsub_v1.PublisherClient()
        self.project_id = project_id
        self.topic_id = topic_id

    def get_topic_path(self):
        return self.client.topic_path(self.project_id, self.topic_id)

    def publish_message(self, data_dict: dict):
        data_bytes = GCPPublisherConnection._convert_data_dict_to_message_bytes(data_dict)
        try:
            publish_future = self.client.publish(self.get_topic_path(), data=data_bytes)
            publish_future.result()
            return True
        except Exception as e:
            logging.error(e)
            return False

    @staticmethod
    def _convert_data_dict_to_message_bytes(data_dict: dict):
        data_json = json.dumps(data_dict)
        data_bytes = data_json.encode('utf-8')
        return data_bytes
