import shlex
import subprocess
from os import path

import setuptools


class Packages:
    ELASTICSEARCH = "elasticsearch[async]~=7.6"
    HALO = "halo~=0.0"
    IMPORTLIB_RESOURCES = "importlib-resources~=3.2"
    MARSHMALLOW_DATACLASS = "marshmallow-dataclass~=8.5"
    MARSHMALLOW_ENUM = "marshmallow-enum~=1.5"
    MORE_ITERTOOLS = "more-itertools~=8.9"
    PY_YAML = "PyYaml>=3.11,<6"
    PYSPARK = "pyspark==2.4.5"
    TOML = "toml~=0.10"
    TYPING_EXTENSIONS = "typing-extensions~=4.1"
    GDCMODELS = "gdcmodels @ git+ssh://git@github.com/NCI-GDC/gdc-models.git@2.8.1-rc.2#egg=gdcmodels"
    INDEXCLIENT = "indexclient @ git+https://github.com/NCI-GDC/indexclient.git@2.0.0#egg=indexclient"
    MUTATIONINDEXERRESOURCE = "mutationindexerresource @ git+ssh://git@github.com/NCI-GDC/mutation-indexer-resource.git@civic_annot#egg=mutationindexerresource"
    NORMALIZER = "normalizer @ git+ssh://git@github.com/NCI-GDC/normalizer.git@2.0.4#egg=normalizer"


root_dir = path.dirname(__file__)
git_file = path.abspath(path.join(root_dir, ".git"))
git_hash = (
    subprocess.check_output(shlex.split(f"git --git-dir={git_file} rev-parse HEAD"))
    .decode("utf-8")
    .strip()
)

setuptools.setup(
    name="mutation-indexer",
    use_scm_version={
        "local_scheme": "dirty-tag",
        "write_to": "_version.py",
    },
    setup_requires=["setuptools_scm<6"],
    description="Spark driver for extracting viz indices data.",
    license="Apache",
    packages=setuptools.find_packages(exclude=("tests", "tests.*")),
    include_package_data=True,
    options={"egg_info": {"tag_build": f".rev.{git_hash}"}},
    install_requires=(
        Packages.ELASTICSEARCH,
        Packages.IMPORTLIB_RESOURCES,
        Packages.MARSHMALLOW_DATACLASS,
        Packages.MARSHMALLOW_ENUM,
        Packages.MORE_ITERTOOLS,
        Packages.PYSPARK,
        Packages.PY_YAML,
        Packages.TOML,
        Packages.TYPING_EXTENSIONS,
        Packages.INDEXCLIENT,
        Packages.NORMALIZER,
        Packages.GDCMODELS,
    ),
    extras_require={
        "master": (Packages.HALO,),
        "gene-expression": (),
        "viz": (Packages.MUTATIONINDEXERRESOURCE,),
    },
)
