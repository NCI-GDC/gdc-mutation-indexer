from base import BaseArgs


class BuildArgs(BaseArgs):
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
            'nargs': 1,  # FIXME: how to "1 or 2"?
            'type': int,
            'default': [0],
        },
        'pipelines': {
            'help': 'List of pipelines to build',
            'nargs': '*',
            'default': ['mutect', 'muse', 'varscan', 'somaticsniper', 'FM'],
        },
        'debug': {
            'help': 'Debug mode. More explicit logging but slower.',
            'action': 'store_true',
        },
        'store-raw': {
            'help': 'Store raw json indices in S3',
            'action': 'store_true',
        },
        'load-raw': {
            'help': 'Load raw json indices from S3 to DataFrame',
            'action': 'store_true',
        },
        'no-overwrite-raw': {
            'help': 'If set, will not overwrite raw indices previously stored in S3',
            'action': 'store_false',
        },
        'projects': {
            'help': 'Subset of projects to build. If not provided, builds all',
            'nargs': '*',
            'default': [],
        },
    }
