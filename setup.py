import os
import shlex
from subprocess import check_output
from setuptools import setup, find_packages
from config import (
    VERSION,
    ROOT_DIR,
    GIT_HEAD_REV,
)

PACKAGES = find_packages()

setup(
    name="gdc-mutation-indexer",
    version=VERSION,
    description="ETL for mutation elasticsearch indices",
    license="Apache",
    packages=PACKAGES,
    py_modules=["config"],
    include_package_data=True,
    options=dict(egg_info=dict(tag_build=('_rev_' + GIT_HEAD_REV))),
)
