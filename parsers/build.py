from base import Parser


class BuildArgs(Parser):
    """
    Arguments controlling the build
    """
    group = {
        'title': 'Build arguments',
        'description': 'Parameters controlling the build',
    }

    arguments = {
        'build-type': {
            'help': 'Indicates if the build meant for the release. '
                'If release, --label and --version are taken from DataRelease node.',
            'choices': ['release', 'develop'],
            'required': True,
        },
        'build-label': {
            'help': 'Label for the index (will be automatically assigned to the value in '
                'DataRelease.name for release candidate node if --build-type == "release")',
            'default': 'mutation_indexer',
        },
        'build-version': {
            'help': 'Version number (will be automatically assigned to the value in '
                'DataRelease node for release candidate if --build-type == "release")',
            'nargs': '*',
            'type': int,
            'default': [0],
        },
        'projects': {
            'help': 'Subset of projects to build. If not provided, builds all',
            'nargs': '*',
            'default': [],
        },
        'pipelines': {
            'help': 'List of pipelines to build',
            'nargs': '*',
            'default': ['mutect', 'muse', 'varscan', 'somaticsniper', 'FM'],
        },
        'index-types': {
            'help': 'List of pipelines to build',
            'nargs': '*',
            'default': [
                'case_centric', 'gene_centric',
                'ssm_centric', 'ssm_occurrence_centric',
                'cnv_centric', 'cnv_occurrence_centric',
            ],
        },
        'maf-backup': {
            'help': 'Whether to read maf_df from backup or rebuild and write or do nothing',
            'choices': ['read', 'write', 'neither'],
            'default': 'read',
        },
        'gistic-backup': {
            'help': 'Whether to read gistic_df from backup or rebuild and write or do nothing',
            'choices': ['read', 'write', 'neither'],
            'default': 'read',
        },
        'output-raw': {
            'help': 'Whether to read raw output indices from backup or rebuild and write or do nothing',
            'choices': ['read', 'write', 'neither'],
            'default': 'neither',
        },
        'debug': {
            'help': 'Debug mode. More explicit logging but slower.',
            'action': 'store_true',
        },
    }
