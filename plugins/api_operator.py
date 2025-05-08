import requests
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class APIOperator(BaseOperator):
    @apply_defaults
    def __init__(self, endpoint: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.endpoint = endpoint  # API endpoint

    def execute(self, context):
        # Interact with the API
        response = requests.get(self.endpoint)
        if response.status_code == 200:
            self.log.info(f"API response: {response.text}")
        else:
            self.log.error(f"Failed to fetch API data: {response.status_code}")
