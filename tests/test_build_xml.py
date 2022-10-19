import os

from dotenv import load_dotenv
from datetime import datetime
import pytest
import boto3

from src.build_xml import BuildXML

load_dotenv()


@pytest.fixture()
def get_current_datetime():
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
def start_build_xml(get_current_datetime):
    build_xml = BuildXML(
        email=os.getenv("EMAIL_NOTIFICATION"),
        date_time=get_current_datetime,
        group_name='Testing group name'
    )
    return build_xml


@pytest.fixture()
def get_initial_data():
    return {
        "Email": os.getenv("EMAIL_NOTIFICATION"),
        "Confirm": "False",
        "Detail": "False",
        "Subject": "OFF",
        "Return": "true",
        "Group": "Testing Group Name",
        "Action": "OFF",
        "Campaign": "OFF",
        "HtmlMessage": "False",
        "DisableMessageOptions": "OFF",
    }


def test_initial_data(start_build_xml):
    assert start_build_xml.get_data()["Email"] == os.getenv("EMAIL_NOTIFICATION")
    assert start_build_xml.get_data()["Confirm"] == "False"
    assert start_build_xml.get_data()["Detail"] == "False"
    assert start_build_xml.get_data()["HtmlMessage"] == "False"
    assert start_build_xml.get_data()["Subject"] == "OFF"
    assert start_build_xml.get_data()["Action"] == "OFF"
    assert start_build_xml.get_data()["Campaign"] == "OFF"
    assert start_build_xml.get_data()["DisableMessageOptions"] == "OFF"


def test_writing_file(start_build_xml, get_initial_data, get_current_datetime):
    start_build_xml.write_file(data=get_initial_data)
    assert os.path.isfile(f"IN_{get_current_datetime}.xml")


def test_send_file(start_build_xml, get_initial_data, get_current_datetime, start_boto3_client):
    start_build_xml.write_file(data=get_initial_data)
    start_build_xml.send_file()

    assert f"IN_{get_current_datetime}.xml" in [c["Key"] for c in start_boto3_client.list_objects(
        Bucket='xml-file-uploading'
    )["Contents"]]
