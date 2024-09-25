from setuptools import setup

setup(
    name="gdc-mutation-indexer",
    url="https://github.com/NCI-GDC/gdc-mutation-indexer",
    author="NCI GDC",
    author_email="gdc_dev_questions-aaaaae2lhsbell56tlvh3upgoq@cdis.slack.com",
    use_scm_version={
        "local_scheme": "dirty-tag",
        "write_to": "_version.py",
    },
    setup_requires=["setuptools_scm<6"],
    description="ETL for mutation elasticsearch indices",
    long_description_content_type="text/markdown",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
    ],
    license="Apache",
    include_package_data=True,
    install_requires=[
        "elasticsearch[async]~=7.6",
        "importlib-resources~=3.2",
        "marshmallow-dataclass~=8.5",
        "marshmallow-enum~=1.5",
        "more-itertools~=8.9",
        "networkx",
        "pyspark~=3.3.1",
        "python-json-logger~=2.0",
        # `setuptools` is required for references to `pkg_resources` in mutation
        # indexer itself and in normalizer/gdc-models.
        "setuptools",
        "toml~=0.10",
        "typing-extensions~=4.1",
        "indexclient",
        "gdcmodels",
        "mutation-indexer-resource @ git+ssh://git@github.com/NCI-GDC/mutation-indexer-resource.git@1.0.0",
    ],
    extras_require={
        "dev": (
            "click~=8.1",
            "coverage[toml]~=7.0",
            "deepdiff~=6.0",
            "pytest~=7.0",
            "pytest-cov~=4.0",
        ),
        "client": ("halo", "pex", "wheel"),
    },
    entry_points={
        "console_scripts": ("mutation-indexer = mutation_indexer.client:main [client]",)
    },
)
