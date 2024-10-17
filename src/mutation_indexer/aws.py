import io

import boto3

from mutation_indexer.configuration import aws


class S3Util:
    __slots__ = ("_client",)

    def __init__(self, config: aws.S3) -> None:
        self._client = boto3.client("s3")

    def write(self, bucket: str, key: str, data: bytes) -> None:
        self._client.upload_fileobj(io.BytesIO(data), Bucket=bucket, Key=key)
