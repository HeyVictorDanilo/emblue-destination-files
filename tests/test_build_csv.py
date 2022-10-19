import os
import pytest

from datetime import datetime
import pandas as pd
import boto3

from src.build_csv import BuildCSV


@pytest.fixture()
def get_current_datetime() -> str:
    return datetime.now().strftime('%Y%m%d%H%M')


@pytest.fixture()
def start_boto3_client():
    return boto3.client(
        service_name="s3",
        region_name=os.getenv("REGION"),
        aws_access_key_id=os.getenv("ACCESS_KEY"),
        aws_secret_access_key=os.getenv("SECRET_KEY")
    )


@pytest.fixture()
def starting_build_csv(get_current_datetime):
    build_csv = BuildCSV(date_time=get_current_datetime)
    return build_csv


@pytest.fixture()
def get_starting_dataframe(starting_build_csv):
    return starting_build_csv.get_dataframe()


def test_get_connection(starting_build_csv):
    assert starting_build_csv.get_connection()


def test_get_dataframe(starting_build_csv):
    assert isinstance(starting_build_csv.get_dataframe(), pd.DataFrame)


def test_update_dataframe(starting_build_csv, get_starting_dataframe):
    df = starting_build_csv.update_data(dataframe=get_starting_dataframe)

    assert 'FECHA_ENVIO' in df.columns
    assert 'FECHA_ACTIVIDAD' in df.columns
    assert 'CAMPANIA' in df.columns
    assert 'ACCION' in df.columns
    assert 'TIPO_ACCION' in df.columns
    assert 'ACTIVIDAD' in df.columns
    assert 'DESCRIPCION' in df.columns
    assert 'TAG' in df.columns


def test_send_file(starting_build_csv, get_starting_dataframe, get_current_datetime, start_boto3_client):
    df = starting_build_csv.update_data(dataframe=get_starting_dataframe)
    starting_build_csv.send_file(data_frame=df)
    assert f"IN_{get_current_datetime}.csv" in [c["Key"] for c in start_boto3_client.list_objects(
        Bucket='xml-file-uploading'
    )["Contents"]]
