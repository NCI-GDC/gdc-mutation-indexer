from base import BaseArgs


class S3Args(BaseArgs):
    """
    S3 arguments
    """
    args = {
        's3_host',
        's3_bucket',
        's3_access_key',
        's3_secret_key',
    }

    def add_args(self, parser):
        spark_args = parser.add_argument_group(
            title='S3 arguments',
            description='Object store credentials'
        )
        spark_args.add_argument(
            '--s3-host', help='S3 host',
            required=True,
        )
        spark_args.add_argument(
            '--s3-access-key', help='S3 access key',
            required=True,
        )
        spark_args.add_argument(
            '--s3-secret-key', help='S3 secret key',
            required=True,
        )
        spark_args.add_argument( # NOTE: will be obsolete when we merge reading mafs from es index
            '--s3-bucket', help='S3 host',
            default='somatic-maf',
        )
        return parser
