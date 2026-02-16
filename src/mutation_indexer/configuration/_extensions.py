"""A module for extending basic functionality of the various configuration objects."""

from __future__ import annotations

import copy
import dataclasses
import pathlib
from collections.abc import Callable, Iterable, Mapping
from os import path
from typing import Any, ClassVar, Final, Generic, Self, TypeVar, cast, dataclass_transform

import marshmallow
import marshmallow_dataclass
from deepmerge import merger
from marshmallow import exceptions, fields, schema, utils

T = TypeVar("T")


class ArrayTupleField(fields.Field):
    __slots__ = ("_inner",)

    def __init__(
        self,
        inner: fields.Field | type[fields.Field],
        *,
        load_default: Any = utils.missing,
        missing: Any = utils.missing,
        dump_default: Any = utils.missing,
        default: Any = utils.missing,
        data_key: str | None = None,
        attribute: str | None = None,
        validate: Callable[[Any], Any] | Iterable[Callable[[Any], Any]] | None = None,
        required: bool = False,
        allow_none: bool | None = None,
        load_only: bool = False,
        dump_only: bool = False,
        error_messages: dict[str, str] | None = None,
        metadata: Mapping[str, Any] | None = None,
        **additional_metadata,
    ) -> None:
        """A field representing a tuple containing an unspecified number of elements.

        NOTE: This field is largely based on `fields.List`.

        Args:
            inner: The field which represents the elements contained within the tuple.
            ...: For all other args, please refer to marshmallow's `fields.Field` for
                further details.
        """
        super().__init__(
            load_default=load_default,
            missing=missing,
            dump_default=dump_default,
            default=default,
            data_key=data_key,
            attribute=attribute,
            validate=validate,
            required=required,
            allow_none=allow_none,
            load_only=load_only,
            dump_only=dump_only,
            error_messages=error_messages,
            metadata=metadata,
            **additional_metadata,
        )

        self._inner = utils.resolve_field_instance(inner)

    def _bind_to_schema(self, field_name: str, schema: schema.Schema | fields.Field) -> None:
        self._inner = copy.copy(self._inner)

        super()._bind_to_schema(field_name, schema)
        self._inner._bind_to_schema(field_name, self)

    def _serialize(self, value: Any, attr: str | None, obj: Any, **kwargs) -> Any:
        return tuple(self._inner._serialize(v, attr, obj, **kwargs) for v in value)

    def _deserialize_values(self, value: Iterable[Any], **kwargs) -> Iterable[Any]:
        errors = {}

        for idx, each in enumerate(value):
            try:
                yield self._inner.deserialize(each, **kwargs)
            except exceptions.ValidationError as error:
                if error.valid_data is not None:
                    yield error.valid_data
                errors.update({idx: error.messages})

        if errors:
            raise exceptions.ValidationError(errors)

    def _deserialize(
        self,
        value: Any,
        attr: str | None,
        data: Mapping[str, Any] | None,
        **kwargs: Any,
    ) -> tuple:
        if not utils.is_collection(value):
            raise self.make_error("invalid")

        return tuple(self._deserialize_values(value, **kwargs))


class ResolvedPathField(fields.Field):
    """A field for loading paths and resolving any environment vars within.

    NOTE: when serialized environment variables from the original string are not
    preserved.

    e.g.
        "$HOME/config" -> pathlib.Path("/home/user/config")
    """

    def _serialize(
        self,
        value: Any,
        attr: str | None,
        obj: Any,
        **kwargs: Any,
    ) -> str:
        return str(value)

    def _deserialize(
        self,
        value: Any,
        attr: str | None,
        data: Mapping[str, Any] | None,
        **kwargs: Any,
    ) -> pathlib.Path:
        if not isinstance(value, str):
            exceptions.ValidationError(
                f"Invalid value for path: {value}.",
                field_name=attr or exceptions.SCHEMA,
            )

        return pathlib.Path(path.expandvars(value)).expanduser()


class SecretStringField(fields.String):
    """A field for insuring sensitive strings are obfuscated when serializing."""

    def _serialize(self, value, attr, obj, **kwargs) -> str | None:
        assert self.root, "Invalid context."

        if self.root.context.get("is_obfuscated"):
            value = "*" * len(value)

        return super()._serialize(value, attr, obj, **kwargs)


class FormatMapRootField(fields.String):
    """A field which applies `format_map(root_data)` to the deserialized value.

    E.g.
        "{build[data_release]}" -> "drXX"
    """

    def _serialize(
        self,
        value: str,
        attr: str | None,
        obj: Any,
        **kwargs: Any,
    ) -> str:
        # Ensure that escaped values are returned to original markup.
        return value.replace("{", "{{").replace("}", "}}")

    def _deserialize(
        self,
        value: Any,
        attr: str | None,
        data: Mapping[str, Any] | None,
        **kwargs: Any,
    ) -> str:
        assert self.root, "Field must have a root schema."

        root_data = self.root.context.get("root_data", {})
        template: str = super()._deserialize(value, attr, data, **kwargs)

        return template.format_map(root_data)


class Schema(Generic[T]):
    def __init__(self, cls: type[T]) -> None:
        """A schema which handles the serialization/deserialization of T.

        Args:
            cls: The class which the schema will load/dump.
        """
        self._schema = marshmallow_dataclass.class_schema(cls)(
            many=False, unknown=marshmallow.EXCLUDE
        )

    def load(self, data: Mapping[str, Any]) -> T:
        """Loads the given data into an instance of T.

        Args:
            data: The data being loaded into the instance.

        Returns:
            A validated instance of T.
        """
        self._schema.context["root_data"] = data
        obj = cast(T, self._schema.load(data))
        _ = self._schema.context.pop("root_data")

        return obj

    def dump(self, obj: T, is_obfuscated: bool) -> Mapping[str, Any]:
        """Dumps the given instance into a mapping representation of the data.

        Args:
            obj: The object whose data should be translated into a mapping.
            is_obfuscated: A flag indicating if secret strings should be obfuscated in
                the translation process.

        Returns:
            A mapping of the data contained within the given instance.
        """
        self._schema.context["is_obfuscated"] = is_obfuscated
        data = cast(Mapping[str, Any], self._schema.dump(obj))
        _ = self._schema.context.pop("is_obfuscated")

        return data


@dataclass_transform(frozen_default=True)
class SerializableDataclass:
    """A class representing a dataclass which can be (de)serialized."""

    _schema: ClassVar[Schema[Self]]
    """The schema which should be used for serialization."""
    _merger: Final[merger.Merger] = merger.Merger(
        ((dict, ["merge"]), (list, ["override"]), (set, ["override"])),
        fallback_strategies=["override"],
        type_conflict_strategies=["override"],
    )
    """A dictionary merger which should be used to load multiple sources into one."""

    def __init_subclass__(cls) -> None:
        """Initializes subclasses ensuring that they are dataclasses & have a schema."""
        dataclasses.dataclass(frozen=True)(cls)

        cls._schema = Schema(cls)
