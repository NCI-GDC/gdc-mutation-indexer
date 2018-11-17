from base import BaseArgs


class S3Args(BaseArgs):
    """
    S3 arguments
    """
    args = {
        's3_host',
        's3_access_key',
        's3_secret_key',
        's3_raw_bucket',
        's3_maf_bucket',
        's3_gistic_bucket',
    }

    def add_args(self, parser):
        s3_args = parser.add_argument_group(
            title='S3 arguments',
            description='Object store credentials'
        )
        s3_args.add_argument(
            '--s3-host', help='S3 host',
            required=True,
        )
        s3_args.add_argument(
            '--s3-access-key', help='S3 access key',
            required=True,
        )
        s3_args.add_argument(
            '--s3-secret-key', help='S3 secret key',
            required=True,
        )
        s3_args.add_argument(# NOTE: will be obsolete when we merge reading mafs from es index
            '--s3-maf-bucket', help='S3 bucket with MAFs',
            default='s3a://somatic-maf/',
        )
        s3_args.add_argument(# NOTE: will be obsolete when we implement reading gistics from es index
            '--s3-gistic-bucket', help='S3 bucket with Gistic files',
            default='s3a://gistic-cnv/',
        )
        s3_args.add_argument(# NOTE: verify that bucket name is correct
            '--s3-raw-bucket', help='S3 bucket with raw json indices',
            default='s3a://mutation_indexer-raw/',
        )

        return parser
