from datetime import datetime
import json
import logging
import os

import boto3
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class BuildCsv:
    def __init__(self):
        self.s3_client = boto3.client(
            service_name="s3",
            region_name=os.getenv("REGION"),
            aws_access_key_id=os.getenv("ACCESS_KEY"),
            aws_secret_access_key=os.getenv("SECRET_KEY")
        )
        self.now = datetime.now()

    def get_data(self):
        pass
