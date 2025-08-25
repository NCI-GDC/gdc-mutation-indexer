import io

import yaml

from mutation_indexer import es_utils


def translate(
    mappings_file: io.TextIOBase, output_file: io.TextIOBase, included_properties: str
) -> None:
    schema_loader = es_utils.SchemaLoader()
    mappings = yaml.safe_load(mappings_file)
    properties_set = (
        frozenset(included_properties.split(","))
        if included_properties
        else mappings.get("properties", {}).keys()
    )
    mappings["properties"] = {
        p: d for p, d in mappings.get("properties", {}).items() if p in properties_set
    }

    yaml.safe_dump(
        schema_loader.load(mappings, source_filter=True, include_as_arrays=()).jsonValue(),
        output_file,
    )
