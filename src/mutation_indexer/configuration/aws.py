"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer+Configuration
"""
import dataclasses

from mutation_indexer.configuration import marshmallow_extensions


@dataclasses.dataclass(frozen=True)
class S3:
    """
    Configuration values for interacting with aws S3 resources.
    """

    host: str
    access_key: str
    secret_key: marshmallow_extensions.SecretString


@dataclasses.dataclass(frozen=True)
class AWS:
    """
    Configuration values for aws resources.
    """

    s3: S3
