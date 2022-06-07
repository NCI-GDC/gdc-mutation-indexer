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
    name="mutation-indexer-core",
    use_scm_version={
        "local_scheme": "dirty-tag",
        "root": "..",
        "relative_to": __file__,
        "write_to": "_version.py",
    },
    setup_requires=["setuptools_scm<6"],
    description="Spark driver for extracting viz indices data.",
    license="Apache",
    packages=setuptools.find_namespace_packages(include=["mutation_indexer.core"]),
    include_package_data=True,
    options={"egg_info": {"tag_build": f".rev.{git_hash}"}},
    install_requires=[
        "marshmallow-dataclass~=8.5",
        "marshmallow-enum~=1.5",
    ],
)
