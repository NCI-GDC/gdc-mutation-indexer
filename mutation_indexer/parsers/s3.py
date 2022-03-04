from mutation_indexer.parsers import base


class S3Args(base.BaseParser):
    """
    S3 arguments
    """

    @property
    def group(self):
        return {
            "title": "S3 arguments",
            "description": "Object store credentials",
        }

    @property
    def arguments(self):
        return {
            "s3-host": {
                "help": "S3 host",
                "required": True,
            },
            "s3-access-key": {
                "help": "S3 access key",
                "required": True,
            },
            "s3-secret-key": {
                "help": "S3 secret key",
                "required": True,
            },
            "s3-maf-bucket": {  # NOTE: will be obsolete when we merge reading mafs from es index
                "help": "S3 bucket with MAFs",
                "default": "s3a://somatic-maf/",
            },
            "s3-gistic-bucket": {  # NOTE: will be obsolete when we implement reading gistics from es index
                "help": "S3 bucket with Gistic files",
                "default": "s3a://gistic-cnv/",
            },
            "s3-raw-bucket": {
                "help": "S3 bucket with raw json indices",
                "default": "s3a://mutation-indexer-raw/",
            },
        }
