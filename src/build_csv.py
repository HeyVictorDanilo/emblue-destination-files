import logging
import os

from dotenv import load_dotenv
from awswrangler import exceptions as awswrangler_exceptions
import awswrangler as wr
import pandas as pd

load_dotenv()

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class BuildCSV:
    def __init__(self, date_time: str):
        self.date_time: str = date_time

    @staticmethod
    def get_connection() -> wr.postgresql.connect:
        try:
            conn = wr.postgresql.connect(
                secret_id=str(os.getenv("SECRET_ARN"))
            )
        except awswrangler_exceptions.InvalidConnection as error:
            logger.error(f'Invalid connection: {error}')
        except Exception as error:
            logger.error(f'Exception: {error}')
        else:
            return conn

    def get_dataframe(self) -> pd.DataFrame:
        try:
            df = wr.postgresql.read_sql_query(
                sql="SELECT email FROM user_company LIMIT 100;",
                con=self.get_connection()
            )
        except awswrangler_exceptions.QueryFailed as error:
            logger.error(f"Query failed error: {error}")
        except Exception as error:
            logger.error(f"Exception: {error}")
        else:
            return df

    @staticmethod
    def update_data(dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe['FECHA_ENVIO'] = ''
        dataframe['FECHA_ACTIVIDAD'] = ''
        dataframe['CAMPANIA'] = ''
        dataframe['ACCION'] = ''
        dataframe['TIPO_ACCION'] = ''
        dataframe['ACTIVIDAD'] = ''
        dataframe['DESCRIPCION'] = ''
        dataframe['TAG'] = ''
        return dataframe

    def send_file(self, data_frame: pd.DataFrame) -> None:
        try:
            wr.s3.to_csv(
                data_frame,
                f"s3://{os.getenv('BUCKET')}/IN_{self.date_time}.csv",
                index=False
            )
        except awswrangler_exceptions.InvalidConnection as error:
            logger.error(f'Invalid connection during uploading file: {error}')
        except awswrangler_exceptions.InvalidFile as error:
            logger.error(f'Invalid file to uploading: {error}')
        except awswrangler_exceptions.EmptyDataFrame as error:
            logger.error(f'Invalid empty data frame: {error}')
        except awswrangler_exceptions.AlreadyExists as error:
            logger.error(f'Invalid object already exists: {error}')
        else:
            pass

    def handler(self) -> None:
        self.send_file(
            data_frame=self.update_data(dataframe=self.get_dataframe())
        )


if __name__ == '__main__':
    build_csv = BuildCSV(date_time='202210181619')
    build_csv.handler()
