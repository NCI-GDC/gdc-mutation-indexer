import copy
import typing
from marshmallow import utils, fields, exceptions, validate


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
