import os
import logging

import paramiko
import boto3
from paramiko.ssh_exception import SSHException

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class UploadFilesSFTP:
    def __init__(self):
        self.s3_client = boto3.client(
            service_name="s3",
            region_name=os.getenv("REGION"),
            aws_access_key_id=os.getenv("ACCESS_KEY"),
            aws_secret_access_key=os.getenv("SECRET_KEY")
        )

    def get_files(self):
        return [
            content["Key"]
            for content in self.s3_client.list_objects(Bucket = 'xml-file-uploading')["Contents"]
        ]

    def handler(self):
        print(self.get_files())
        #self.send_files(files=self.get_files())

    def send_files(self, files):
        try:
            transport = paramiko.Transport(os.environ['HOSTNAME'], 22)
            transport.connect(username=os.environ['username'], password=os.environ['password'])
        except SSHException as error:
            logging.error(error)
        else:
            with paramiko.SFTPClient.from_transport(transport) as sftp:
                with sftp.open('/upload/testing.csv', 'wb', 32768) as f:
                    self.s3_client.download_fileobj(os.getenv("BUCKET"), event["file_name"], f)

        message = 'Hello from Lambda3!'
        return {'body': message}

if __name__ == '__main__':
    uploadin = UploadFilesSFTP().handler()