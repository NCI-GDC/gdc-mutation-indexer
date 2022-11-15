import shlex
import subprocess
from os import path

from setuptools import find_packages, setup

here = path.abspath(path.dirname(__file__))
with open(path.join(here, "README.md")) as f:
    long_description = f.read()


git_hash = (
    subprocess.check_output(
        shlex.split("git --git-dir={}/.git rev-parse HEAD".format(here))
    )
    .decode("utf-8")
    .strip()
)


setup(
    name="gdc-mutation-indexer",
    use_scm_version={
        "local_scheme": "dirty-tag",
        "write_to": "_version.py",
    },
    setup_requires=["setuptools_scm<6"],
    description="ETL for mutation elasticsearch indices",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="Apache",
    packages=find_packages(exclude=("tests.*", "tests")),
    py_modules=["config"],
    include_package_data=True,
    options=dict(egg_info=dict(tag_build=(".rev." + git_hash))),
    install_requires=[
        "boto==2.49.0",
        "elasticsearch[async]~=7.6",
        "importlib-resources~=3.2",
        "ndjson~=0.3",
        "networkx<=2.4",
        "marshmallow-dataclass~=8.5",
        "marshmallow-enum~=1.5",
        "more-itertools~=8.9",
        "pyspark==3.2.2",
        "python-dateutil~=2.8",
        "PyYaml>=3.11,<6",
        "requests~=2.7",
        "six~=1.15.0",
        "toml~=0.10",
        "typing-extensions~=4.1",
        "gdcdictionary @ git+https://github.com/NCI-GDC/gdcdictionary.git@2.4.0#egg=gdcdictionary",
        "gdcdatamodel @ git+https://github.com/NCI-GDC/gdcdatamodel.git@3.4.0#egg=gdcdatamodel",
        "indexclient @ git+https://github.com/NCI-GDC/indexclient.git@2.0.0#egg=indexclient",
        "gdcmodels @ git+ssh://git@github.com/NCI-GDC/gdc-models.git@2.10.0-rc.1#egg=gdcmodels",
        "normalizer @ git+ssh://git@github.com/NCI-GDC/normalizer.git@2.1.1#egg=normalizer",
        "mutationindexerresource @ git+ssh://git@github.com/NCI-GDC/mutation-indexer-resource.git@civic_annot#egg=mutationindexerresource",
    ],
)
