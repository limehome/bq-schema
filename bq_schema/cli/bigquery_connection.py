import json
import os

from google.cloud import bigquery
from google.cloud.bigquery import Client
from google.oauth2 import service_account


def create_connection() -> Client:
    service_file_json = os.environ.get("GOOGLE_SERVICE_FILE")
    if service_file_json:
        service_file = json.loads(service_file_json)
        credentials = service_account.Credentials.from_service_account_info(
            service_file
        )
        return Client(credentials=credentials, project=service_file["project_id"])

    return bigquery.Client()
