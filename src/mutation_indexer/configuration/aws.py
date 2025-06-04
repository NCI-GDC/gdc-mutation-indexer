"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer+Configuration
"""

import dataclasses
import pathlib
from typing import Annotated, Optional

from mutation_indexer.configuration import _extensions


@dataclasses.dataclass(frozen=True)
class S3:
    """
    Configuration values for interacting with aws S3 resources.
    """

    host: str
    access_key: str
    secret_key: Annotated[str, _extensions.SecretStringField]
    ca_certs: Annotated[
        Optional[pathlib.Path],
        _extensions.ResolvedPathField(
            validate=_extensions.PathValidator(
                is_optional=True, is_file=True, exists=True
            )
        ),
    ] = None
    validate: Optional[bool] = None


@dataclasses.dataclass(frozen=True)
class AWS:
    """
    Configuration values for aws resources.
    """

    s3: S3
