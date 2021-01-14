import boto3
from botocore.client import Config
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
import logging
import fileoperation as fp
import time

log_file = 'log.txt'
report_file = "report.txt"
version_file="version.txt"

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler(log_file)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - line: %(lineno)d - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

cf = Config(connect_timeout=1500, read_timeout=300)
GB = 1024 ** 3
config = TransferConfig(multipart_threshold=5*GB, use_threads=False)

class S3ud():
    def __init__(self, access_key, secret_key):
        self.url = 'http://localhost:8083'
        self.s3_client = boto3.client(service_name='s3', aws_access_key_id=access_key,
                                      aws_secret_access_key=secret_key, endpoint_url=self.url, verify=False, config=cf)
    def upload(self, bucket_name, file_name):
        try:
            resp = self.s3_client.put_object(
                Bucket=bucket_name,
                Key=file_name,
                Body=open(file_name, 'rb').read()
            )
        except ClientError as e:
            logger.error(e)
            return None
        return resp


    def upload_file(self, file_name, bucket_name, object_name=None):
        if object_name is None:
            object_name = file_name
        try:
            resp = self.s3_client.upload_file(file_name, bucket_name, object_name)
        except ClientError as e:
            logger.error(e)
            return None
        return resp


    def download(self, bucket_name, file_name, d_file_name):
        try:
            self.s3_client.download_file(bucket_name, file_name, d_file_name, Config=config)
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                print "download_file_%s_Fail." % file_name
                logger.error("download_file_%s_Fail." % file_name)
                fp.append_report(report_file, "[%s] - download_file_%s_Fail\n" % (time.strftime("%Y-%m-%d %H:%M:%S"), file_name))
            else:
                raise

        # resp = self.s3_client.get_object(
        #     Bucket='polly',
        #     Key='20201111_1M00.txt'
        # )
        # with open('./1M.txt', 'wb') as f:
        #     f.write(resp['Body'].read())
        #with open(d_file_name, 'wb') as f:
        #    self.s3_client.download_fileobj(bucket_name, file_name, f, Config=config)

if __name__ == "__main__":
    s3 = S3ud("", "")
