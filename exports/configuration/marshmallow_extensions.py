import typing

import marshmallow
from marshmallow import exceptions, fields, utils


class ArbitraryLengthTuple(fields.List):
    def _deserialize_values(
        self, value: typing.Iterable[typing.Any], **kwargs
    ) -> typing.Iterable[typing.Any]:
        errors = {}

        for idx, each in enumerate(value):
            try:
                yield self.inner.deserialize(each, **kwargs)
            except exceptions.ValidationError as error:
                if error.valid_data is not None:
                    yield error.valid_data
                errors.update({idx: error.messages})

        if errors:
            raise exceptions.ValidationError(errors)

    def _deserialize(
        self, value, attr, data, **kwargs
    ) -> typing.Tuple[typing.Any, ...]:
        if not utils.is_collection(value):
            raise self.make_error("invalid")

        return tuple(self._deserialize_values(value, **kwargs))


class SecretString(str):
    ...


class SecretStringField(fields.String):
    def _serialize(self, value, attr, obj, **kwargs) -> typing.Optional[str]:
        if self.root.context.get("is_obfuscated"):
            value = "*" * len(value)

        return super()._serialize(value, attr, obj, **kwargs)


class ExtendedSchema(marshmallow.Schema):
    TYPE_MAPPING = {SecretString: SecretStringField}
