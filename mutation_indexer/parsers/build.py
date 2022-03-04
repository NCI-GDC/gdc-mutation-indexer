from mutation_indexer.parsers import base


class BuildArgs(base.BaseParser):
    """
    Arguments controlling the build
    """

    @property
    def group(self):
        return {
            "title": "Build arguments",
            "description": "Parameters controlling the build",
        }

    @property
    def arguments(self):
        return {
            "build-type": {
                "help": "Not currently supported.",
                "choices": ["release", "develop"],
                "default": "develop",
            },
            "build-label": {
                "help": "Label for the index",
                "default": "mutation_indexer",
            },
            "build-version": {
                "help": "Not currently supported.",
                "nargs": "*",
                "type": int,
                "default": [0],
            },
            "gencode-version": {
                "help": "The gencode version to build with",
                "default": "v22",
            },
            "study-label": {
                "help": "Label for the controlled-access study associated with the new "
                "indices. Omit if the indices will be open-access.",
                "default": "",
            },
            "projects": {
                "help": "Subset of projects to build. If not provided, builds all",
                "nargs": "*",
                "default": [],
            },
            "pipelines": {
                "help": "List of pipelines to build",
                "nargs": "*",
                "default": ["mutect", "muse", "varscan", "somaticsniper", "FM"],
            },
            "index-types": {
                "help": "List of pipelines to build",
                "nargs": "*",
                "default": [
                    "case_centric",
                    "gene_centric",
                    "ssm_centric",
                    "ssm_occurrence_centric",
                    "cnv_centric",
                    "cnv_occurrence_centric",
                    "gene_expression",
                ],
            },
            "maf-backup": {
                "help": "Whether to read maf_df from backup or rebuild and write or do nothing",
                "choices": ["read", "write", "neither"],
                "default": "read",
            },
            "gistic-backup": {
                "help": "Whether to read gistic_df from backup or rebuild and write or do nothing",
                "choices": ["read", "write", "neither"],
                "default": "read",
            },
            "gene-expression-cases-backup": {
                "help": "Whether to read gene_expression_cases_df from backup or rebuild and write"
                " or do nothing",
                "choices": ["read", "write", "neither"],
                "default": "neither",
            },
            "gene-expression-values-backup": {
                "help": "Whether to read gene_expression_values_df from backup or rebuild and write"
                " or do nothing",
                "choices": ["read", "write", "neither"],
                "default": "neither",
            },
            "primary-aliquot-backup": {
                "help": "Whether to read primary_aliquot_df from backup or rebuild and write"
                " or do nothing",
                "choices": ["read", "write", "neither"],
                "default": "neither",
            },
            "gene-model-backup": {
                "help": "Whether to read gene_model_df from backup or rebuild and write"
                " or do nothing",
                "choices": ["read", "write", "neither"],
                "default": "neither",
            },
            "ascat-backup": {
                "help": "Whether to read ascat_df from backup or rebuild and write"
                " or do nothing",
                "choices": ["read", "write", "neither"],
                "default": "neither",
            },
            "output-raw": {
                "help": "Whether to read raw output indices from backup or rebuild and write or do nothing",
                "choices": ["read", "write", "neither"],
                "default": "neither",
            },
            "debug": {
                "help": "Debug mode. More explicit logging but slower.",
                "action": "store_true",
            },
            "include-maf-urls": {
                "help": "Add additional maf urls that might not be in the graph",
                "nargs": "*",
                "default": [],
            },
            "blacklist-fields": {
                "help": "Specify additional fields to be excluded when loading graph index case df",
                "nargs": "*",
                "default": [],
            },
            "skip-normalization": {
                "help": "Use mappings without normalizers",
                "action": "store_true",
            },
            "skip-es-mafs": {
                "help": "Do not query elasticsearch for MAFs. Should be used together with --include-maf-urls option",
                "action": "store_true",
            },
            "omit-cnv-data": {
                "help": "Omit all cnv data from the built indices.",
                "action": "store_true",
            },
        }
