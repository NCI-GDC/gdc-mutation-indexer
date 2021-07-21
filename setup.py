from setuptools import setup, find_packages

PACKAGES = find_packages()

setup(
    name="gdc-mutation-indexer",
    use_scm_version={"local_scheme": "dirty-tag", "write_to": "_version.py"},
    setup_requires=["setuptools_scm<6"],
    description="ETL for mutation elasticsearch indices",
    license="Apache",
    packages=find_packages(),
    py_modules=["config"],
    include_package_data=True,
    install_requires=[
        "boto==2.49.0",
        "elasticsearch~=7.6",
        "networkx<=2.4",
        "requests~=2.7",
        "python-dateutil~=2.8",
        "PyYaml>=3.11,<6",
        "six~=1.15.0",
        "psqlgraph @ git+https://github.com/NCI-GDC/psqlgraph.git@3.3.0#egg=psqlgraph",
        "gdcdictionary @ git+https://github.com/NCI-GDC/gdcdictionary.git@2.4.0#egg=gdcdictionary",
        "gdcdatamodel @ git+https://github.com/NCI-GDC/gdcdatamodel.git@3.4.0#egg=gdcdatamodel",
        "indexclient @ git+https://github.com/NCI-GDC/indexclient.git@2.0.0#egg=indexclient",
        "gdcmodels @ git+ssh://git@github.com/NCI-GDC/gdc-models.git@feat/dev-807-boveri-mappings#egg=gdcmodels",
        "normalizer @ git+ssh://git@github.com/NCI-GDC/normalizer.git@2.0.4#egg=normalizer",
    ],
)
