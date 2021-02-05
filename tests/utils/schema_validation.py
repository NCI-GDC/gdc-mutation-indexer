class SchemaDataType(object):
    
    def __init__(
        self,
        class_name,
        fields=tuple(),
        elementType=None,
        keyType=None,
        valueType=None
    ):
        self.class_name = class_name
        self.fields = tuple(SchemaField(**f) for f in fields)
        self.elementType = SchemaDataType(**elementType) if elementType else None
        self.keyType = SchemaDataType(**keyType) if keyType else None
        self.valueType = SchemaDataType(**valueType) if valueType else None


class SchemaField(object):

    def __init__(self, name, dataType):
        self.name = name
        self.dataType = SchemaDataType(**dataType)


class Schema(object):

    def __init__(self, fields):
        self.fields = tuple(
            SchemaField(**f) for f in fields
        )


class PysaprkSchemaValidator(object):
    STRUCT_TYPE = "StructType"
    ARRAY_TYPE = "ArrayType"
    MAP_TYPE = "MapType"

    def _fields_to_dict(self, schema):
        return {
            f.name: f for f in schema.fields
        }

    def _validate_data_type(self, actual_data_type, expected_data_type, field_name):
        """
        Validates the the actual data type is the one that is expected as well as any
        subtypes associated with it.
        
        """
        actual_name = actual_data_type.__class__.__name__
        expected_name = expected_data_type.class_name

        assert actual_name == expected_name, (
            "Data types do not match for field: {}\nActual Type: {}\nExpected Type: {}".format(
                field_name,
                actual_name,
                expected_name
            )
        )

        if actual_name == self.ARRAY_TYPE:
            self._validate_array(actual_data_type, expected_data_type, field_name)

        elif actual_name == self.STRUCT_TYPE:
            self._validate_struct(actual_data_type, expected_data_type, field_name)

        elif actual_name == self.MAP_TYPE:
            self._validate_map(actual_data_type, expected_data_type, field_name)

    def _validate_array(self, actual_array, expected_array, field_name):
        """
        Validates that the actual array data type's element type is the one that is expected.

        """
        self._validate_data_type(
            actual_array.elementType,
            expected_array.elementType,
            "{}.{}".format(field_name, "elementType"),
        )

    def _validate_map(self, actual_map, expected_map, field_name):
        """
        Validates that the actual map data type's key type and value type are the ones that are 
        expected.
        
        """
        self._validate_data_type(
            actual_map.keyType,
            expected_map.keyType,
            "{}.{}".format(field_name, "keyType"),
        )
        self._validate_data_type(
            actual_map.valueType,
            expected_map.valueType,
            "{}.{}".format(field_name, "valueType"),
        )

    def _validate_field(self, actual_field, expected_field, parent_name):
        """
        Validates that the field has the expected name as well as the underlying data type are the 
        expected values.
        
        """
        name = "{}.{}".format(parent_name, expected_field.name)
        
        assert actual_field is not None, "Missing field: {}".format(name)

        self._validate_data_type(actual_field.dataType, expected_field.dataType, name)

    def _validate_struct(self, actual_struct, expected_struct, parent_name):
        actual_fields = self._fields_to_dict(actual_struct)
        expected_fields = self._fields_to_dict(expected_struct)

        assert len(actual_fields) == len(expected_fields), (
            "Fields do not match:\nactual({}): {}\nexpected({}): {}".format(
                len(actual_fields),
                actual_fields.keys(),
                len(expected_fields),
                expected_fields.keys(),
            )
        )

        for key, expected_field in expected_fields.items():
            actual_field = actual_fields.get(key)
        
            self._validate_field(actual_field, expected_field, parent_name)

    def validate_schema(self, actual_schema, expected_schema):
        """
        Validates the actual PySpark schema against an expected Schema.

        Args:
            actual_schema (pyspark.sql.types.StructType): The actual schema returned by the dataframe
            expected_schema (Schema): A basic representation of the expected schema structure

        """
        self._validate_struct(actual_schema, expected_schema, "$")
        
