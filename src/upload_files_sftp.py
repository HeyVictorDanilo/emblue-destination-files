from typing import List, Any, Dict
import os
import logging

from paramiko.ssh_exception import SSHException
from dotenv import load_dotenv
import paramiko
import boto3

load_dotenv()

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class UploadFilesSFTP:
    def __init__(self) -> None:
        self.s3_client = boto3.client(
            service_name="s3",
            region_name=os.getenv("REGION"),
            aws_access_key_id=os.getenv("ACCESS_KEY"),
            aws_secret_access_key=os.getenv("SECRET_KEY")
        )

    def get_files(self) -> List[str]:
        return [
            content["Key"]
            for content in self.s3_client.list_objects(Bucket = 'xml-file-uploading')["Contents"]
        ]

    def handler(self) -> None:
        self.send_files(files=self.get_files())

    def send_files(self, files: List[str]) -> Dict[str, Any]:
        for file in files:
            try:
                transport = paramiko.Transport(os.getenv("SFTP_HOSTNAME"), 22)
                transport.connect(username=os.getenv('SFTP_USER_NAME'), password=os.getenv('SFTP_PASSWORD'))
            except SSHException as error:
                logging.error(error)
            else:
                with paramiko.SFTPClient.from_transport(transport) as sftp:
                    with sftp.open(f'/upload/{file}', 'wb', 32768) as f:
                        self.s3_client.download_fileobj(os.getenv("BUCKET"), file, f)

        message = 'Hello from Lambda3!'
        return {'body': message}


if __name__ == '__main__':
    uploading = UploadFilesSFTP()
    uploading.handler()