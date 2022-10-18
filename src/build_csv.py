from datetime import datetime
import json
import logging
import os

from dotenv import load_dotenv
import awswrangler as wr
import pandas as pd
import boto3
import pg8000.native

load_dotenv()

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class BuildCsv:
    def __init__(self):
        self.now = datetime.now()

    @staticmethod
    def get_dataframe():
        conn = wr.postgresql.connect(
            secret_id=str(os.getenv("SECRET_ARN"))
        )
        df = wr.postgresql.read_sql_query(
            sql="SELECT email FROM user_company LIMIT 100;",
            con=conn
        )
        return df

    @staticmethod
    def update_data(dataframe):
        dataframe['FECHA_ENVIO'] = ''
        dataframe['FECHA_ACTIVIDAD'] = ''
        dataframe['CAMPANIA'] = ''
        dataframe['ACCION'] = ''
        dataframe['TIPO_ACCION'] = ''
        dataframe['ACTIVIDAD'] = ''
        dataframe['DESCRIPCION'] = ''
        dataframe['TAG'] = ''
        return dataframe

    def handler(self):
        print(self.update_data(dataframe=self.get_dataframe()))


if __name__ == '__main__':
    build_csv = BuildCsv()
    print(build_csv.handler())