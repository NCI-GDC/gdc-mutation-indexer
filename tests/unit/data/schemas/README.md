# Schemas
The schemas module offers commands for managing and creating and managing schemas used 
to load test inputs. Though, it can be useful in some cases for production schemas; 
these will be found in the main export/schemas directory.

- [Minimize](#minimize)
- [Translate Mapping](#translate-mapping)

## Minimize
This command uses anchors to minimize the size of the yaml schema to reduce the amount
of redundant lines which are common in the schema structures.

### Example
File Input:
```yaml
fields:
- metadata: {}
  name: model_id
  nullable: true
  type: string
- metadata: {}
  name: prop
  nullable: true
  type: string
type: struct
```
Bash Input:
```bash
python -m tests.unit.data.schemas minimize \
    -s ./input.yaml # NOTE: This can be: a file, a directory, or a module. Default: tests.unit.data.schemas
```

File Output:
```yaml
fields:
- &string_field
  metadata: {}
  name: model_id
  nullable: true
  type: string
- <<: *string_field
  name: prop
type: struct
```

## Translate Mapping
This command attempts to create a schema from a Elasticsearch mapping file. This script
should be used with caution due to Elasticsearch not differentiating between arrays and
singular objects of atomic types. E.g. a list of terms as well as a singular `keyword` 
in an Elasticsearch mapping both have a type of `keyword`

### Example
#### Inputs
File:
```yaml
properties:
  model_id:
    type: keyword
  prop:
    type: long
  ignored:
    type: boolean
```
Command:
```bash
python -m tests.unit.data.schemas translate-mapping \
    -m ./mapping.yaml \
    -o ./output.yaml \
    -i ignored  # a comma separated list of props to ignore.
```

#### Outputs
File:
```yaml
fields:
- metadata: {}
  name: model_id
  nullable: true
  type: string
- metadata: {}
  name: prop
  nullable: true
  type: long
type: struct
```
