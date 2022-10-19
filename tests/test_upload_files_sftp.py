from typing import List
import os

from paramiko.ssh_exception import SSHException
import pytest
import paramiko

from src.upload_files_sftp import UploadFilesSFTP


@pytest.fixture()
def start_upload_files():
    upload_files = UploadFilesSFTP()
    return upload_files


@pytest.fixture()
def get_object_buckets(start_upload_files):
    return start_upload_files.get_files()


def test_objects_key(start_upload_files) -> List[str]:
    assert isinstance(start_upload_files.get_files(), list)


def test_send_files(start_upload_files, get_object_buckets):
    start_upload_files.send_files(files=get_object_buckets)

    try:
        transport = paramiko.Transport(os.getenv("SFTP_HOSTNAME"), 22)
        transport.connect(username=os.getenv('SFTP_USER_NAME'), password=os.getenv('SFTP_PASSWORD'))
    except SSHException as error:
        print(error)
    else:
        with paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.chdir('upload')
            sftp_list_objects = sftp.listdir()

    for object_file in get_object_buckets:
        assert object_file in sftp_list_objects