from setuptools import find_packages, setup

setup(
    name="gdc-mutation-indexer",
    use_scm_version={
        "local_scheme": "dirty-tag",
        "write_to": "_version.py",
    },
    setup_requires=["setuptools_scm<6"],
    description="ETL for mutation elasticsearch indices",
    long_description_content_type="text/markdown",
    license="Apache",
    packages=find_packages(exclude=("tests.*", "tests")),
    py_modules=["config"],
    include_package_data=True,
    install_requires=[
        "elasticsearch[async]~=7.6",
        "importlib-resources~=3.2",
        "marshmallow-dataclass~=8.5",
        "marshmallow-enum~=1.5",
        "more-itertools~=8.9",
        "pyspark==3.3.1",
        "python-json-logger~=2.0",
        # `setuptools` is required for references to `pkg_resources` in mutation
        # indexer itself and in normalizer/gdc-models.
        "setuptools",
        "toml~=0.10",
        "typing-extensions~=4.1",
        "indexclient @ git+https://github.com/NCI-GDC/indexclient.git@2.3.2#egg=indexclient",
        "gdcmodels @ git+https://github.com/NCI-GDC/gdc-models.git@4.0.0#egg=gdcmodels",
        "normalizer @ git+ssh://git@github.com/NCI-GDC/normalizer.git@4.0.0#egg=normalizer",
        "mutationindexerresource @ git+ssh://git@github.com/NCI-GDC/mutation-indexer-resource.git@civic_annot#egg=mutationindexerresource",
    ],
)
