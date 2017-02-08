from setuptools import setup, find_packages

setup(
    name="gdc-mutation-indexer",
    version="0.1.0",
    description="ETL for mutation elasticsearch indices",
    license="Apache",
    packages=find_packages(),
    py_modules=["config"],
    package_data={'': ['*.yml','exports/schemas/maf.yml']}
)
