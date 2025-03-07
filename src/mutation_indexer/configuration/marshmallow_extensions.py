"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer+Configuration
"""

import pathlib
import typing
from os import path

import marshmallow_dataclass
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
        self,
        value: typing.Any,
        attr: str | None,
        data: typing.Mapping[str, typing.Any] | None,
        **kwargs: typing.Any,
    ):
        if not utils.is_collection(value):
            raise self.make_error("invalid")

        return tuple(self._deserialize_values(value, **kwargs))


class ResolvedPathField(fields.Field):
    def _serialize(
        self,
        value: typing.Any,
        attr: str | None,
        obj: typing.Any,
        **kwargs: typing.Any,
    ):
        return str(value)

    def _deserialize(
        self,
        value: typing.Any,
        attr: str | None,
        data: typing.Mapping[str, typing.Any] | None,
        **kwargs: typing.Any,
    ):
        if not isinstance(value, str):
            exceptions.ValidationError(
                f"Invalid value for path: {value}.",
                field_name=attr or exceptions.SCHEMA,
            )

        return pathlib.Path(path.expandvars(value)).expanduser()


class SecretStringField(fields.String):
    def _serialize(self, value, attr, obj, **kwargs) -> str | None:
        assert self.root, "Invalid context."

        if self.root.context.get("is_obfuscated"):
            value = "*" * len(value)

        return super()._serialize(value, attr, obj, **kwargs)


ResolvedPath = marshmallow_dataclass.NewType(
    "Path", typ=pathlib.Path, field=ResolvedPathField
)


SecretString = marshmallow_dataclass.NewType(
    "SecretString", typ=str, field=SecretStringField
)
