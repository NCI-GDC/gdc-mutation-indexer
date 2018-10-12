from setuptools import setup, find_packages
import shlex
from subprocess import check_output

GIT_HEAD_REV = check_output(shlex.split('git rev-parse HEAD')).strip()
PACKAGES = find_packages()

setup(
    name="gdc-mutation-indexer",
    version="0.1.0",
    description="ETL for mutation elasticsearch indices",
    license="Apache",
    packages=PACKAGES,
    py_modules=["config", "config_utils"],
    include_package_data=True,
    options=dict(egg_info=dict(tag_build=('_rev_' + GIT_HEAD_REV))),
)

