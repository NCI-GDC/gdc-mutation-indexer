from base import BaseArgs


class BuildArgs(BaseArgs):
    """
    Arguments controlling the build
    """
    args = {
        'build_type',
        'build_label',
        'build_version',
        'projects',
        'pipelines',
        'no_overwrite_raw',
        'store_raw',
        'load_raw',
        'debug',
    }

    def add_args(self, parser):
        build_args = parser.add_argument_group(
            title='Build arguments',
            description='Parameters controlling the build'
        )
        build_args.add_argument(
            '--build-type', help='Indicates if the build meant for the release. '
            'If release, --label and --version are taken from DataRelease node.',
            choices=['release', 'develop'],
            required=True,
        )
        build_args.add_argument(
            '--build-label', help='Label for the index (will be automatically assigned to the value in '
            'DataRelease.name for release candidate node if --build-type == "release")',
            default='mutation_indexer',
        )
        build_args.add_argument(
            '--build-version',
            help='Version number (will be automatically assigned to the value in '
            'DataRelease node for release candidate if --build-type == "release")',
            nargs=1,
            type=int,
            default=[0],
        )
        build_args.add_argument(
            '--pipelines',
            help="List of pipelines to build",
            nargs='*',
            default=['mutect', 'muse', 'varscan', 'somaticsniper', 'FM'],
        )
        build_args.add_argument(
            '--debug',
            help="Debug mode. More explicit logging but slower.",
            action='store_true',
        )
        build_args.add_argument(
            '--store-raw',
            help="Store raw json indices in S3",
            action='store_true',
        )
        build_args.add_argument(
            '--load-raw',
            help="Load raw json indices from S3 to DataFrame",
            action='store_true',
        )
        build_args.add_argument(
            '--no-overwrite-raw',
            help="If set, will not overwrite raw indices previously stored in S3",
            action='store_true',
        )
        build_args.add_argument(
            '--projects',
            help="Subset of projects to build. If not provided, builds all",
            nargs='*',
            default=[],
        )

        return parser
