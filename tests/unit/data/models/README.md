# Models
The models module offers commands for managing and creating data models representing
data that will be input into tests.

### Contents
- [Create Models](#create-models)

## Create Models
Create models is a command that will create a model based on a given schema. It will
populate that data with default values given in the defaults provided to the command.
Any values for which there is no default provided `None` will be used. Substructures are
included as nested classes withing their parent object to prevent namespace collisions.

### Example
#### Inputs
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

Command:
```bash
python -m tests.unit.data.models create-model \
    -n ModelClsName \  # The class name for the root model
    -s ./schema.yaml \  # The schema upon which to base the model.
    -d ./defaults.yaml \  # A mapping of prop names to default values.
    -o ./model.py \  # The output python file to write the models to.
    --include-asserts  # Tells the command to create asserts to validate row data against model data.
```

#### Outputs
model.py (comments for clarification only):
```python
@dataclasses.dataclass(frozen=True)
class ModelClsName:  # name from command line
    # properties from the schema.yaml struct
    model_id: str | None = "model-0"  # default value from defaults.yaml
    prop: int | None = None

    # included because of --insert-asserts/-a flag
    def assert_equals(self, row: sql.Row) -> bool:
        assert row
        assert row.model_id == self.model_id
        assert row.prop == self.prop

        return True
```
