import shlex
import subprocess
from os import path

import setuptools

root_dir = path.dirname(__file__)
git_file = path.abspath(path.join(root_dir, "../.git"))
git_hash = (
    subprocess.check_output(shlex.split(f"git --git-dir={git_file} rev-parse HEAD"))
    .decode("utf-8")
    .strip()
)


setuptools.setup(
    name="mutation-indexer-driver",
    use_scm_version={
        "local_scheme": "dirty-tag",
        "root": "..",
        "relative_to": __file__,
        "write_to": "_version.py",
    },
    setup_requires=["setuptools_scm<6"],
    description="Spark driver for extracting viz indices data.",
    license="Apache",
    packages=setuptools.find_namespace_packages(include=["mutation_indexer.*"]),
    include_package_data=True,
    options={"egg_info": {"tag_build": f".rev.{git_hash}"}},
    install_requires=[
        "elasticsearch[async]~=7.6",
        "importlib-resources~=3.2",
        "pyspark==2.4.5",
        "PyYaml>=3.11,<6",
        "typing-extensions~=4.1",
        "mutation-indexer-core",
        "psqlgraph @ git+https://github.com/NCI-GDC/psqlgraph.git@3.3.0#egg=psqlgraph",
        "indexclient @ git+https://github.com/NCI-GDC/indexclient.git@2.0.0#egg=indexclient",
        "normalizer @ git+ssh://git@github.com/NCI-GDC/normalizer.git@2.0.4#egg=normalizer",
        "gdcmodels @ git+ssh://git@github.com/NCI-GDC/gdc-models.git@2.8.1-rc.2#egg=gdcmodels",
    ],
)
