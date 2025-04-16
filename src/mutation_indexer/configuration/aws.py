"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer+Configuration
"""

import dataclasses
from typing import Annotated

from mutation_indexer.configuration import _extensions


@dataclasses.dataclass(frozen=True)
class S3:
    """
    Configuration values for interacting with aws S3 resources.
    """

    host: str
    access_key: str
    secret_key: Annotated[str, _extensions.SecretStringField]


@dataclasses.dataclass(frozen=True)
class AWS:
    """
    Configuration values for aws resources.
    """

    s3: S3
