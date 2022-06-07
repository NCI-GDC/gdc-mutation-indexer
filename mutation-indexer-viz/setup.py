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
    name="mutation-indexer-viz",
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
    options=dict(egg_info=dict(tag_build=(".rev." + git_hash))),
    install_requires=[
        "elasticsearch[async]~=7.6",
        "pyspark==2.4.5",
        "python-dateutil~=2.8",
        "PyYaml>=3.11,<6",
        "toml~=0.10",
        "typing-extensions~=4.1",
        "mutation-indexer-core",
        "mutation-indexer-driver",
        "indexclient @ git+https://github.com/NCI-GDC/indexclient.git@2.0.0#egg=indexclient",
        "mutationindexerresource @ git+ssh://git@github.com/NCI-GDC/mutation-indexer-resource.git@civic_annot#egg=mutationindexerresource",
        "gdcmodels @ git+ssh://git@github.com/NCI-GDC/gdc-models.git@2.8.1-rc.2#egg=gdcmodels",
    ],
)
