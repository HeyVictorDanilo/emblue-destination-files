import os

import pytest
from dotenv import load_dotenv
from datetime import datetime

from src.build_xml import BuildXML

load_dotenv()


@pytest.fixture()
def start_build_xml():
    build_xml = BuildXML(
        email=os.getenv("EMAIL_NOTIFICATION"),
        date_time=datetime.now().strftime('%Y%m%d%H%M'),
        group_name='Testing group name'
    )
    return build_xml


def test_initial_data(start_build_xml):
    assert start_build_xml.get_data()["Email"] == os.getenv("EMAIL_NOTIFICATION")
    assert start_build_xml.get_data()["Confirm"] == "False"
    assert start_build_xml.get_data()["Detail"] == "False"
    assert start_build_xml.get_data()["HtmlMessage"] == "False"
    assert start_build_xml.get_data()["Subject"] == "OFF"
    assert start_build_xml.get_data()["Action"] == "OFF"
    assert start_build_xml.get_data()["Campaign"] == "OFF"
    assert start_build_xml.get_data()["DisableMessageOptions"] == "OFF"