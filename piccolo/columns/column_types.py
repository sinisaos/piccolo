"""
Notes for devs
==============

Descriptors
-----------

Each column type implements the descriptor protocol (the ``__get__`` and
``__set__`` methods).

This is to signal to MyPy that the following is allowed:

.. code-block:: python

    class Band(Table):
        name = Varchar()

    band = Band()
    band.name = 'Pythonistas'  # Without descriptors, this would be an error

In the above example, descriptors allow us to tell MyPy that ``name`` is a
``Varchar`` when accessed on a class, but is a ``str`` when accessed on a class
instance.

"""

from __future__ import annotations

import copy
import decimal
import inspect
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    Optional,
    Union,
    cast,
    overload,
)

from typing_extensions import Unpack

from piccolo.columns.base import (
    Column,
    ColumnKwargs,
    ForeignKeyMeta,
    OnDelete,
    OnUpdate,
    ReferencedTable,
)
from piccolo.columns.combination import Where
from piccolo.columns.defaults.date import DateArg, DateCustom, DateNow
from piccolo.columns.defaults.interval import IntervalArg, IntervalCustom
from piccolo.columns.defaults.time import TimeArg, TimeCustom, TimeNow
from piccolo.columns.defaults.timestamp import (
    TimestampArg,
    TimestampCustom,
    TimestampNow,
)
from piccolo.columns.defaults.timestamptz import (
    TimestamptzArg,
    TimestamptzCustom,
    TimestamptzNow,
)
from piccolo.columns.defaults.uuid import UUID4, UUIDArg
from piccolo.columns.operators.comparison import (
    ArrayAll,
    ArrayAny,
    ArrayNotAny,
)
from piccolo.columns.operators.string import Concat
from piccolo.columns.reference import LazyTableReference
from piccolo.querystring import QueryString
from piccolo.utils.encoding import dump_json
from piccolo.utils.warnings import colored_warning

if TYPE_CHECKING:  # pragma: no cover
    from piccolo.columns.base import ColumnMeta
    from piccolo.query.functions.array import ArrayItemType, ArrayType
    from piccolo.query.operators.json import (
        GetChildElement,
        GetElementFromPath,
    )
    from piccolo.table import Table


###############################################################################


class ConcatDelegate:
    """
    Used in update queries to concatenate two strings - for example::

        await Band.update({Band.name: Band.name + 'abc'})

    """

    def get_querystring(
        self,
        column: Column,
        value: Union[str, Column, QueryString],
        reverse: bool = False,
    ) -> QueryString:
        """
        :param reverse:
            By default the value is appended to the column's value. If
            ``reverse=True`` then the value is prepended to the column's
            value instead.

        """
        if isinstance(value, Column):
            if len(column._meta.call_chain) > 0:
                raise ValueError(
                    "Adding values across joins isn't currently supported."
                )
        elif isinstance(value, str):
            value = QueryString("CAST({} AS TEXT)", value)
        elif not isinstance(value, QueryString):
            raise ValueError(
                "Only str, Column and QueryString values can be added."
            )

        args = [value, column] if reverse else [column, value]

        # We use the concat operator instead of the concat function, because
        # this is what we historically used, and they treat null values
        # differently.
        return QueryString(
            Concat.template.format(value_1="{}", value_2="{}"), *args
        )


class MathDelegate:
    """
    Used in update queries to perform math operations on columns, for example::

        await Band.update({Band.popularity: Band.popularity + 100})

    """

    def get_querystring(
        self,
        column_name: str,
        operator: Literal["+", "-", "/", "*"],
        value: Union[int, float, Integer],
        reverse: bool = False,
    ) -> QueryString:
        if isinstance(value, Integer):
            column: Integer = value
            if len(column._meta.call_chain) > 0:
                raise ValueError(
                    "Adding values across joins isn't currently supported."
                )
            column_name = column._meta.db_column_name
            return QueryString(f"{column_name} {operator} {column_name}")
        elif isinstance(value, (int, float)):
            if reverse:
                return QueryString(f"{{}} {operator} {column_name}", value)
            else:
                return QueryString(f"{column_name} {operator} {{}}", value)
        else:
            raise ValueError(
                "Only integers, floats, and other Integer columns can be "
                "added."
            )


class TimedeltaDelegate:
    """
    Used in update queries to add a timedelta to these columns:

    * ``Timestamp``
    * ``Timestamptz``
    * ``Date``
    * ``Interval``

    Example::

        class Concert(Table):
            starts = Timestamp()

        # Lets us increase all of the matching values by 1 day:
        >>> await Concert.update({
        ...     Concert.starts: Concert.starts + datetime.timedelta(days=1)
        ... })

    """

    # Maps the attribute name in Python's timedelta to what it's called in
    # Postgres.
    postgres_attr_map: dict[str, str] = {
        "days": "DAYS",
        "seconds": "SECONDS",
        "microseconds": "MICROSECONDS",
    }

    def get_postgres_interval_string(self, interval: timedelta) -> str:
        """
        :returns:
            A string like::

                "'1 DAYS 5 SECONDS 1000 MICROSECONDS'"

        """
        output = []
        for timedelta_key, postgres_name in self.postgres_attr_map.items():
            timestamp_value = getattr(interval, timedelta_key)
            if timestamp_value:
                output.append(f"{timestamp_value} {postgres_name}")

        output_string = " ".join(output)
        return f"'{output_string}'"

    def get_sqlite_interval_string(self, interval: timedelta) -> str:
        """
        :returns:
            A string like::

                "'+1 DAYS', '+5.001 SECONDS'"

        """
        output = []

        data = {
            "DAYS": interval.days,
            "SECONDS": interval.seconds + (interval.microseconds / 10**6),
        }

        for key, value in data.items():
            if value:
                operator = "+" if value >= 0 else ""
                output.append(f"'{operator}{value} {key}'")

        output_string = ", ".join(output)
        return output_string

    def get_querystring(
        self,
        column: Column,
        operator: Literal["+", "-"],
        value: timedelta,
        engine_type: str,
    ) -> QueryString:
        column_name = column._meta.name

        if not isinstance(value, timedelta):
            raise ValueError("Only timedelta values can be added.")

        if engine_type in ("postgres", "cockroach"):
            value_string = self.get_postgres_interval_string(interval=value)
            return QueryString(
                f'"{column_name}" {operator} INTERVAL {value_string}',
            )
        elif engine_type == "sqlite":
            if isinstance(column, Interval):
                # SQLite doesn't have a proper Interval type. Instead we store
                # the number of seconds.
                return QueryString(
                    f'CAST("{column_name}" AS REAL) {operator} {value.total_seconds()}'  # noqa: E501
                )
            elif isinstance(column, (Timestamp, Timestamptz)):
                if (
                    round(value.microseconds / 1000) * 1000
                    != value.microseconds
                ):
                    raise ValueError(
                        "timedeltas with such high precision won't save "
                        "accurately - the max resolution is 1 millisecond."
                    )
                strftime_format = "%Y-%m-%d %H:%M:%f"
            elif isinstance(column, Date):
                strftime_format = "%Y-%m-%d"
            else:
                raise ValueError(
                    f"{column.__class__.__name__} doesn't support timedelta "
                    "addition currently."
                )

            if operator == "-":
                value = value * -1

            value_string = self.get_sqlite_interval_string(interval=value)

            # We use `strftime` instead of `datetime`, because `datetime`
            # doesn't return microseconds.
            return QueryString(
                f"strftime('{strftime_format}', \"{column_name}\", {value_string})"  # noqa: E501
            )
        else:
            raise ValueError("Unrecognised engine")


###############################################################################


class Varchar(Column):
    """
    Used for storing text when you want to enforce character length limits.
    Uses the ``str`` type for values.

    **Example**

    .. code-block:: python

        class Band(Table):
            name = Varchar(length=100)

        # Create
        >>> await Band(name='Pythonistas').save()

        # Query
        >>> await Band.select(Band.name)
        {'name': 'Pythonistas'}

    :param length:
        The maximum number of characters allowed.

    """

    value_type = str
    concat_delegate: ConcatDelegate = ConcatDelegate()

    def __init__(
        self,
        length: Optional[int] = 255,
        default: Union[str, Enum, Callable[[], str], None] = "",
        **kwargs: Unpack[ColumnKwargs],
    ) -> None:
        self._validate_default(default, (str, None))

        self.length = length
        self.default = default
        super().__init__(length=length, default=default, **kwargs)

    @property
    def column_type(self):
        return f"VARCHAR({self.length})" if self.length else "VARCHAR"

    ###########################################################################
    # For update queries

    def __add__(self, value: Union[str, Varchar, Text]) -> QueryString:
        return self.concat_delegate.get_querystring(
            column=self,
            value=value,
        )

    def __radd__(self, value: Union[str, Varchar, Text]) -> QueryString:
        return self.concat_delegate.get_querystring(
            column=self,
            value=value,
            reverse=True,
        )

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> str: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> Varchar: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[str, None]):
        obj.__dict__[self._meta.name] = value


class Email(Varchar):
    """
    Used for storing email addresses. It's identical to :class:`Varchar`,
    except when using :func:`create_pydantic_model <piccolo.utils.pydantic.create_pydantic_model>` -
    we add email validation to the Pydantic model. This means that :ref:`PiccoloAdmin`
    also validates email addresses.
    """  # noqa: E501

    pass


class Secret(Varchar):
    """
    This is just an alias to ``Varchar(secret=True)``. It's here for backwards
    compatibility.
    """

    def __init__(self, *args, **kwargs):
        kwargs["secret"] = True
        super().__init__(*args, **kwargs)

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> str: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> Secret: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[str, None]):
        obj.__dict__[self._meta.name] = value


class Text(Column):
    """
    Use when you want to store large strings, and don't want to limit the
    string size. Uses the ``str`` type for values.

    **Example**

    .. code-block:: python

        class Band(Table):
            name = Text()

        # Create
        >>> await Band(name='Pythonistas').save()

        # Query
        >>> await Band.select(Band.name)
        {'name': 'Pythonistas'}

    """

    value_type = str
    concat_delegate: ConcatDelegate = ConcatDelegate()

    def __init__(
        self,
        default: Union[str, Enum, None, Callable[[], str]] = "",
        **kwargs: Unpack[ColumnKwargs],
    ) -> None:
        self._validate_default(default, (str, None))
        self.default = default
        super().__init__(default=default, **kwargs)

    ###########################################################################
    # For update queries

    def __add__(self, value: Union[str, Varchar, Text]) -> QueryString:
        return self.concat_delegate.get_querystring(
            column=self,
            value=value,
        )

    def __radd__(self, value: Union[str, Varchar, Text]) -> QueryString:
        return self.concat_delegate.get_querystring(
            column=self,
            value=value,
            reverse=True,
        )

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> str: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> Text: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[str, None]):
        obj.__dict__[self._meta.name] = value


class UUID(Column):
    """
    Used for storing UUIDs - in Postgres a UUID column type is used, and in
    SQLite it's just a Varchar. Uses the ``uuid.UUID`` type for values.

    **Example**

    .. code-block:: python

        import uuid

        class Band(Table):
            uuid = UUID()

        # Create
        >>> await DiscountCode(code=uuid.uuid4()).save()

        # Query
        >>> await DiscountCode.select(DiscountCode.code)
        {'code': UUID('09c4c17d-af68-4ce7-9955-73dcd892e462')}

    """

    value_type = uuid.UUID

    def __init__(
        self,
        default: UUIDArg = UUID4(),
        **kwargs: Unpack[ColumnKwargs],
    ) -> None:
        if default is UUID4:
            # In case the class is passed in, instead of an instance.
            default = UUID4()

        self._validate_default(default, UUIDArg.__args__)  # type: ignore

        if default == uuid.uuid4:
            default = UUID4()

        if isinstance(default, str):
            try:
                default = uuid.UUID(default)
            except ValueError as e:
                raise ValueError(
                    "The default is a string, but not a valid uuid."
                ) from e

        self.default = default
        super().__init__(default=default, **kwargs)

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> uuid.UUID: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> UUID: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[uuid.UUID, None]):
        obj.__dict__[self._meta.name] = value


class Integer(Column):
    """
    Used for storing whole numbers. Uses the ``int`` type for values.

    **Example**

    .. code-block:: python

        class Band(Table):
            popularity = Integer()

        # Create
        >>> await Band(popularity=1000).save()

        # Query
        >>> await Band.select(Band.popularity)
        {'popularity': 1000}

    """

    math_delegate = MathDelegate()

    def __init__(
        self,
        default: Union[int, Enum, Callable[[], int], None] = 0,
        **kwargs: Unpack[ColumnKwargs],
    ) -> None:
        self._validate_default(default, (int, None))
        self.default = default
        super().__init__(default=default, **kwargs)

    ###########################################################################
    # For update queries

    def __add__(self, value: Union[int, float, Integer]) -> QueryString:
        return self.math_delegate.get_querystring(
            column_name=self._meta.db_column_name, operator="+", value=value
        )

    def __radd__(self, value: Union[int, float, Integer]) -> QueryString:
        return self.math_delegate.get_querystring(
            column_name=self._meta.db_column_name,
            operator="+",
            value=value,
            reverse=True,
        )

    def __sub__(self, value: Union[int, float, Integer]) -> QueryString:
        return self.math_delegate.get_querystring(
            column_name=self._meta.db_column_name, operator="-", value=value
        )

    def __rsub__(self, value: Union[int, float, Integer]) -> QueryString:
        return self.math_delegate.get_querystring(
            column_name=self._meta.db_column_name,
            operator="-",
            value=value,
            reverse=True,
        )

    def __mul__(self, value: Union[int, float, Integer]) -> QueryString:
        return self.math_delegate.get_querystring(
            column_name=self._meta.db_column_name, operator="*", value=value
        )

    def __rmul__(self, value: Union[int, float, Integer]) -> QueryString:
        return self.math_delegate.get_querystring(
            column_name=self._meta.db_column_name,
            operator="*",
            value=value,
            reverse=True,
        )

    def __truediv__(self, value: Union[int, float, Integer]) -> QueryString:
        return self.math_delegate.get_querystring(
            column_name=self._meta.db_column_name, operator="/", value=value
        )

    def __rtruediv__(self, value: Union[int, float, Integer]) -> QueryString:
        return self.math_delegate.get_querystring(
            column_name=self._meta.db_column_name,
            operator="/",
            value=value,
            reverse=True,
        )

    def __floordiv__(self, value: Union[int, float, Integer]) -> QueryString:
        return self.math_delegate.get_querystring(
            column_name=self._meta.db_column_name, operator="/", value=value
        )

    def __rfloordiv__(self, value: Union[int, float, Integer]) -> QueryString:
        return self.math_delegate.get_querystring(
            column_name=self._meta.db_column_name,
            operator="/",
            value=value,
            reverse=True,
        )

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> int: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> Integer: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[int, None]):
        obj.__dict__[self._meta.name] = value


###############################################################################
# BigInt and SmallInt only exist on Postgres. SQLite treats them the same as
# Integer columns.


class BigInt(Integer):
    """
    In Postgres, this column supports large integers. In SQLite, it's an alias
    to an Integer column, which already supports large integers. Uses the
    ``int`` type for values.

    **Example**

    .. code-block:: python

        class Band(Table):
            value = BigInt()

        # Create
        >>> await Band(popularity=1000000).save()

        # Query
        >>> await Band.select(Band.popularity)
        {'popularity': 1000000}

    """

    def _get_column_type(self, engine_type: str):
        if engine_type == "postgres":
            return "BIGINT"
        elif engine_type == "cockroach":
            return "BIGINT"
        elif engine_type == "sqlite":
            return "INTEGER"
        raise Exception("Unrecognized engine type")

    @property
    def column_type(self):
        return self._get_column_type(engine_type=self._meta.engine_type)

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> int: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> BigInt: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[int, None]):
        obj.__dict__[self._meta.name] = value


class SmallInt(Integer):
    """
    In Postgres, this column supports small integers. In SQLite, it's an alias
    to an Integer column. Uses the ``int`` type for values.

    **Example**

    .. code-block:: python

        class Band(Table):
            value = SmallInt()

        # Create
        >>> await Band(popularity=1000).save()

        # Query
        >>> await Band.select(Band.popularity)
        {'popularity': 1000}

    """

    @property
    def column_type(self):
        engine_type = self._meta.engine_type
        if engine_type == "postgres":
            return "SMALLINT"
        elif engine_type == "cockroach":
            return "SMALLINT"
        elif engine_type == "sqlite":
            return "INTEGER"
        raise Exception("Unrecognized engine type")

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> int: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> SmallInt: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[int, None]):
        obj.__dict__[self._meta.name] = value


###############################################################################


DEFAULT = QueryString("DEFAULT")
NULL = QueryString("null")


class Serial(Column):
    """
    An alias to an autoincrementing integer column in Postgres.
    """

    @property
    def column_type(self):
        engine_type = self._meta.engine_type
        if engine_type == "postgres":
            return "SERIAL"
        elif engine_type == "cockroach":
            return "INTEGER"
        elif engine_type == "sqlite":
            return "INTEGER"
        raise Exception("Unrecognized engine type")

    def default(self):
        engine_type = self._meta.engine_type

        if engine_type == "postgres":
            return DEFAULT
        elif engine_type == "cockroach":
            return QueryString("unique_rowid()")
        elif engine_type == "sqlite":
            return NULL
        raise Exception("Unrecognized engine type")

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> int: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> Serial: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[int, None]):
        obj.__dict__[self._meta.name] = value


class BigSerial(Serial):
    """
    An alias to a large autoincrementing integer column in Postgres.
    """

    @property
    def column_type(self):
        engine_type = self._meta.engine_type
        if engine_type == "postgres":
            return "BIGSERIAL"
        elif engine_type == "cockroach":
            return "BIGINT"
        elif engine_type == "sqlite":
            return "INTEGER"
        raise Exception("Unrecognized engine type")

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> int: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> BigSerial: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[int, None]):
        obj.__dict__[self._meta.name] = value


class PrimaryKey(Serial):
    def __init__(
        self,
        **kwargs: Unpack[ColumnKwargs],
    ) -> None:
        # Set the index to False, as a database should automatically create
        # an index for a PrimaryKey column.
        kwargs.update({"primary_key": True, "index": False})

        colored_warning(
            "`PrimaryKey` is deprecated and will be removed in future "
            "versions. Use `UUID(primary_key=True)` or "
            "`Serial(primary_key=True)` instead. If no primary key column is "
            "specified, Piccolo will automatically add one for you called "
            "`id`.",
            category=DeprecationWarning,
        )

        super().__init__(**kwargs)

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> int: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> PrimaryKey: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[int, None]):
        obj.__dict__[self._meta.name] = value


###############################################################################


class Timestamp(Column):
    """
    Used for storing datetimes. Uses the ``datetime`` type for values.

    **Example**

    .. code-block:: python

        import datetime

        class Concert(Table):
            starts = Timestamp()

        # Create
        >>> await Concert(
        ...    starts=datetime.datetime(year=2050, month=1, day=1)
        ... ).save()

        # Query
        >>> await Concert.select(Concert.starts)
        {'starts': datetime.datetime(2050, 1, 1, 0, 0)}

    """

    value_type = datetime
    timedelta_delegate = TimedeltaDelegate()

    def __init__(
        self,
        default: TimestampArg = TimestampNow(),
        **kwargs: Unpack[ColumnKwargs],
    ) -> None:
        self._validate_default(default, TimestampArg.__args__)  # type: ignore

        if isinstance(default, datetime):
            if default.tzinfo is not None:
                raise ValueError(
                    "Timestamp only stores timezone naive datetime objects - "
                    "use Timestamptz instead."
                )
            default = TimestampCustom.from_datetime(default)

        if default == datetime.now:
            default = TimestampNow()

        self.default = default
        super().__init__(default=default, **kwargs)

    ###########################################################################
    # For update queries

    def __add__(self, value: timedelta) -> QueryString:
        return self.timedelta_delegate.get_querystring(
            column=self,
            operator="+",
            value=value,
            engine_type=self._meta.engine_type,
        )

    def __radd__(self, value: timedelta) -> QueryString:
        return self.__add__(value)

    def __sub__(self, value: timedelta) -> QueryString:
        return self.timedelta_delegate.get_querystring(
            column=self,
            operator="-",
            value=value,
            engine_type=self._meta.engine_type,
        )

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> datetime: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> Timestamp: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[datetime, None]):
        obj.__dict__[self._meta.name] = value


class Timestamptz(Column):
    """
    Used for storing timezone aware datetimes. Uses the ``datetime`` type for
    values. The values are converted to UTC in the database, and are also
    returned as UTC.

    **Example**

    .. code-block:: python

        import datetime

        class Concert(Table):
            starts = Timestamptz()

        # Create
        >>> await Concert(
        ...    starts=datetime.datetime(
        ...        year=2050, month=1, day=1, tzinfo=datetime.timezone.tz
        ...    )
        ... ).save()

        # Query
        >>> await Concert.select(Concert.starts)
        {
            'starts': datetime.datetime(
                2050, 1, 1, 0, 0, tzinfo=datetime.timezone.utc
            )
        }

    """

    value_type = datetime

    # Currently just used by ModelBuilder, to know that we want a timezone
    # aware datetime.
    tz_aware = True

    timedelta_delegate = TimedeltaDelegate()

    def __init__(
        self,
        default: TimestamptzArg = TimestamptzNow(),
        **kwargs: Unpack[ColumnKwargs],
    ) -> None:
        self._validate_default(
            default, TimestamptzArg.__args__  # type: ignore
        )

        if isinstance(default, datetime):
            default = TimestamptzCustom.from_datetime(default)

        if default == datetime.now:
            default = TimestamptzNow()

        self.default = default
        super().__init__(default=default, **kwargs)

    ###########################################################################
    # For update queries

    def __add__(self, value: timedelta) -> QueryString:
        return self.timedelta_delegate.get_querystring(
            column=self,
            operator="+",
            value=value,
            engine_type=self._meta.engine_type,
        )

    def __radd__(self, value: timedelta) -> QueryString:
        return self.__add__(value)

    def __sub__(self, value: timedelta) -> QueryString:
        return self.timedelta_delegate.get_querystring(
            column=self,
            operator="-",
            value=value,
            engine_type=self._meta.engine_type,
        )

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> datetime: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> Timestamptz: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[datetime, None]):
        obj.__dict__[self._meta.name] = value


class Date(Column):
    """
    Used for storing dates. Uses the ``date`` type for values.

    **Example**

    .. code-block:: python

        import datetime

        class Concert(Table):
            starts = Date()

        # Create
        >>> await Concert(
        ...     starts=datetime.date(year=2020, month=1, day=1)
        ... ).save()

        # Query
        >>> await Concert.select(Concert.starts)
        {'starts': datetime.date(2020, 1, 1)}

    """

    value_type = date
    timedelta_delegate = TimedeltaDelegate()

    def __init__(
        self,
        default: DateArg = DateNow(),
        **kwargs: Unpack[ColumnKwargs],
    ) -> None:
        self._validate_default(default, DateArg.__args__)  # type: ignore

        if isinstance(default, date):
            default = DateCustom.from_date(default)

        if default == date.today:
            default = DateNow()

        self.default = default
        super().__init__(default=default, **kwargs)

    ###########################################################################
    # For update queries

    def __add__(self, value: timedelta) -> QueryString:
        return self.timedelta_delegate.get_querystring(
            column=self,
            operator="+",
            value=value,
            engine_type=self._meta.engine_type,
        )

    def __radd__(self, value: timedelta) -> QueryString:
        return self.__add__(value)

    def __sub__(self, value: timedelta) -> QueryString:
        return self.timedelta_delegate.get_querystring(
            column=self,
            operator="-",
            value=value,
            engine_type=self._meta.engine_type,
        )

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> date: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> Date: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[date, None]):
        obj.__dict__[self._meta.name] = value


class Time(Column):
    """
    Used for storing times. Uses the ``time`` type for values.

    **Example**

    .. code-block:: python

        import datetime

        class Concert(Table):
            starts = Time()

        # Create
        >>> await Concert(
        ...    starts=datetime.time(hour=20, minute=0, second=0)
        ... ).save()

        # Query
        >>> await Concert.select(Concert.starts)
        {'starts': datetime.time(20, 0, 0)}

    """

    value_type = time
    timedelta_delegate = TimedeltaDelegate()

    def __init__(
        self,
        default: TimeArg = TimeNow(),
        **kwargs: Unpack[ColumnKwargs],
    ) -> None:
        self._validate_default(default, TimeArg.__args__)  # type: ignore

        if isinstance(default, time):
            default = TimeCustom.from_time(default)

        self.default = default
        super().__init__(default=default, **kwargs)

    ###########################################################################
    # For update queries

    def __add__(self, value: timedelta) -> QueryString:
        return self.timedelta_delegate.get_querystring(
            column=self,
            operator="+",
            value=value,
            engine_type=self._meta.engine_type,
        )

    def __radd__(self, value: timedelta) -> QueryString:
        return self.__add__(value)

    def __sub__(self, value: timedelta) -> QueryString:
        return self.timedelta_delegate.get_querystring(
            column=self,
            operator="-",
            value=value,
            engine_type=self._meta.engine_type,
        )

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> time: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> Time: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[time, None]):
        obj.__dict__[self._meta.name] = value


class Interval(Column):
    """
    Used for storing timedeltas. Uses the ``timedelta`` type for values.

    **Example**

    .. code-block:: python

        from datetime import timedelta

        class Concert(Table):
            duration = Interval()

        # Create
        >>> await Concert(
        ...    duration=timedelta(hours=2)
        ... ).save()

        # Query
        >>> await Concert.select(Concert.duration)
        {'duration': datetime.timedelta(seconds=7200)}

    """

    value_type = timedelta
    timedelta_delegate = TimedeltaDelegate()

    def __init__(
        self,
        default: IntervalArg = IntervalCustom(),
        **kwargs: Unpack[ColumnKwargs],
    ) -> None:
        self._validate_default(default, IntervalArg.__args__)  # type: ignore

        if isinstance(default, timedelta):
            default = IntervalCustom.from_timedelta(default)

        self.default = default
        super().__init__(default=default, **kwargs)

    @property
    def column_type(self):
        engine_type = self._meta.engine_type
        if engine_type in ("postgres", "cockroach"):
            return "INTERVAL"
        elif engine_type == "sqlite":
            # We can't use 'INTERVAL' because the type affinity in SQLite would
            # make it an integer - but we need a text field.
            # https://sqlite.org/datatype3.html#determination_of_column_affinity
            return "SECONDS"
        raise Exception("Unrecognized engine type")

    ###########################################################################
    # For update queries

    def __add__(self, value: timedelta) -> QueryString:
        return self.timedelta_delegate.get_querystring(
            column=self,
            operator="+",
            value=value,
            engine_type=self._meta.engine_type,
        )

    def __radd__(self, value: timedelta) -> QueryString:
        return self.__add__(value)

    def __sub__(self, value: timedelta) -> QueryString:
        return self.timedelta_delegate.get_querystring(
            column=self,
            operator="-",
            value=value,
            engine_type=self._meta.engine_type,
        )

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> timedelta: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> Interval: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[timedelta, None]):
        obj.__dict__[self._meta.name] = value


###############################################################################


class Boolean(Column):
    """
    Used for storing ``True`` / ``False`` values. Uses the ``bool`` type for
    values.

    **Example**

    .. code-block:: python

        class Band(Table):
            has_drummer = Boolean()

        # Create
        >>> await Band(has_drummer=True).save()

        # Query
        >>> await Band.select(Band.has_drummer)
        {'has_drummer': True}

    """

    value_type = bool

    def __init__(
        self,
        default: Union[bool, Enum, Callable[[], bool], None] = False,
        **kwargs: Unpack[ColumnKwargs],
    ) -> None:
        self._validate_default(default, (bool, None))
        self.default = default
        super().__init__(default=default, **kwargs)

    def eq(self, value) -> Where:
        """
        When using ``Boolean`` columns in ``where`` clauses, some Python
        linters don't like it when you do something like:

        .. code-block:: python

            await MyTable.select().where(
                MyTable.some_boolean_column == True
            )

        It's more Pythonic to use ``is True`` rather than ``== True``, which is
        why linters complain. The work around is to do the following instead:

        .. code-block:: python

            await MyTable.select().where(
                MyTable.some_boolean_column.__eq__(True)
            )

        Using the ``__eq__`` magic method is a bit untidy, which is why this
        ``eq`` method exists.

        .. code-block:: python

            await MyTable.select().where(
                MyTable.some_boolean_column.eq(True)
            )

        The ``ne`` method exists for the same reason, for ``!=``.

        """
        return self.__eq__(value)

    def ne(self, value) -> Where:
        """
        See the ``eq`` method for more details.
        """
        return self.__ne__(value)

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> bool: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> Boolean: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[bool, None]):
        obj.__dict__[self._meta.name] = value


###############################################################################


class Numeric(Column):
    """
    Used for storing decimal numbers, when precision is important. An example
    use case is storing financial data. The value is returned as a ``Decimal``.

    **Example**

    .. code-block:: python

        from decimal import Decimal

        class Ticket(Table):
            price = Numeric(digits=(5,2))

        # Create
        >>> await Ticket(price=Decimal('50.0')).save()

        # Query
        >>> await Ticket.select(Ticket.price)
        {'price': Decimal('50.0')}

    :param digits:
        When creating the column, you specify how many digits are allowed
        using a tuple. The first value is the ``precision``, which is the
        total number of digits allowed. The second value is the ``range``,
        which specifies how many of those digits are after the decimal
        point. For example, to store monetary values up to £999.99, the
        digits argument is ``(5,2)``.

    """

    value_type = decimal.Decimal

    @property
    def column_type(self):
        engine_type = self._meta.engine_type
        if engine_type == "cockroach":
            return "NUMERIC"  # All Numeric is the same for Cockroach.
        if self.digits:
            return f"NUMERIC({self.precision}, {self.scale})"
        else:
            return "NUMERIC"

    @property
    def precision(self) -> Optional[int]:
        """
        The total number of digits allowed.
        """
        return self.digits[0] if self.digits is not None else None

    @property
    def scale(self) -> Optional[int]:
        """
        The number of digits after the decimal point.
        """
        return self.digits[1] if self.digits is not None else None

    def __init__(
        self,
        digits: Optional[tuple[int, int]] = None,
        default: Union[
            decimal.Decimal, Enum, Callable[[], decimal.Decimal], None
        ] = decimal.Decimal(0.0),
        **kwargs: Unpack[ColumnKwargs],
    ) -> None:
        if isinstance(digits, tuple):
            if len(digits) != 2:
                raise ValueError(
                    "The `digits` argument should be a tuple of length 2, "
                    "with the first value being the precision, and the second "
                    "value being the scale."
                )
        elif digits is not None:
            raise ValueError("The digits argument should be a tuple.")

        self._validate_default(default, (decimal.Decimal, None))

        self.default = default
        self.digits = digits
        super().__init__(default=default, digits=digits, **kwargs)

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> decimal.Decimal: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> Numeric: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[decimal.Decimal, None]):
        obj.__dict__[self._meta.name] = value


class Decimal(Numeric):
    """
    An alias for Numeric.
    """

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> decimal.Decimal: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> Decimal: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[decimal.Decimal, None]):
        obj.__dict__[self._meta.name] = value


class Real(Column):
    """
    Can be used instead of ``Numeric`` for storing numbers, when precision
    isn't as important. The ``float`` type is used for values.

    **Example**

    .. code-block:: python

        class Concert(Table):
            rating = Real()

        # Create
        >>> await Concert(rating=7.8).save()

        # Query
        >>> await Concert.select(Concert.rating)
        {'rating': 7.8}

    """

    value_type = float

    def __init__(
        self,
        default: Union[float, Enum, Callable[[], float], None] = 0.0,
        **kwargs: Unpack[ColumnKwargs],
    ) -> None:
        if isinstance(default, int):
            # For example, allow `0` as a valid default.
            default = float(default)

        self._validate_default(default, (float, None))
        self.default = default
        super().__init__(default=default, **kwargs)

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> float: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> Real: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[float, None]):
        obj.__dict__[self._meta.name] = value


class Float(Real):
    """
    An alias for Real.
    """

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> float: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> Float: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[float, None]):
        obj.__dict__[self._meta.name] = value


class DoublePrecision(Real):
    """
    The same as ``Real``, except the numbers are stored with greater precision.
    """

    @property
    def column_type(self):
        return "DOUBLE PRECISION"

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> float: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> DoublePrecision: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[float, None]):
        obj.__dict__[self._meta.name] = value


###############################################################################


@dataclass
class ForeignKeySetupResponse:
    is_lazy: bool


class ForeignKey(Column, Generic[ReferencedTable]):
    """
    Used to reference another table. Uses the same type as the primary key
    column on the table it references.

    **Example**

    .. code-block:: python

        class Band(Table):
            manager = ForeignKey(references=Manager)

        # Create
        >>> await Band(manager=1).save()

        # Query
        >>> await Band.select(Band.manager)
        {'manager': 1}

        # Query object
        >>> band = await Band.objects().first()
        >>> band.manager
        1

    **Joins**

    You also use it to perform joins:

    .. code-block:: python

        >>> await Band.select(Band.name, Band.manager.name).first()
        {'name': 'Pythonistas', 'manager.name': 'Guido'}

    To retrieve all of the columns in the related table:

    .. code-block:: python

        >>> await Band.select(Band.name, *Band.manager.all_columns()).first()
        {'name': 'Pythonistas', 'manager.id': 1, 'manager.name': 'Guido'}

    To get a referenced row as an object:

    .. code-block:: python

        manager = await Manager.objects().where(
            Manager.id == some_band.manager
        )

    Or use either of the following, which are just a proxy to the above:

    .. code-block:: python

        manager = await band.get_related('manager')
        manager = await band.get_related(Band.manager)

    To change the manager:

    .. code-block:: python

        band.manager = some_manager_id
        await band.save()

    :param references:
        The ``Table`` being referenced.

        .. code-block:: python

            class Band(Table):
                manager = ForeignKey(references=Manager)

        A table can have a reference to itself, if you pass a ``references``
        argument of ``'self'``.

        .. code-block:: python

            class Musician(Table):
                name = Varchar(length=100)
                instructor = ForeignKey(references='self')

        In certain situations, you may be unable to reference a ``Table`` class
        if it causes a circular dependency. Try and avoid these by refactoring
        your code. If unavoidable, you can specify a lazy reference. If the
        ``Table`` is defined in the same file:

        .. code-block:: python

            class Band(Table):
                manager = ForeignKey(references='Manager')

        If the ``Table`` is defined in a Piccolo app:

        .. code-block:: python

            from piccolo.columns.reference import LazyTableReference

            class Band(Table):
                manager = ForeignKey(
                    references=LazyTableReference(
                       table_class_name="Manager", app_name="my_app",
                    )
                )

        If you aren't using Piccolo apps, you can specify a ``Table`` in any
        Python module:

        .. code-block:: python

            from piccolo.columns.reference import LazyTableReference

            class Band(Table):
                manager = ForeignKey(
                    references=LazyTableReference(
                       table_class_name="Manager",
                       module_path="some_module.tables",
                    )
                    # Alternatively, Piccolo will interpret this string as
                    # the same as above:
                    # references="some_module.tables.Manager"
                )

    :param on_delete:
        Determines what the database should do when a row is deleted with
        foreign keys referencing it. If set to ``OnDelete.cascade``, any rows
        referencing the deleted row are also deleted.

        Options:

        * ``OnDelete.cascade`` (default)
        * ``OnDelete.restrict``
        * ``OnDelete.no_action``
        * ``OnDelete.set_null``
        * ``OnDelete.set_default``

        To learn more about the different options, see the `Postgres docs <https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-FK>`_.

        .. code-block:: python

            from piccolo.columns import OnDelete

            class Band(Table):
                name = ForeignKey(
                    references=Manager,
                    on_delete=OnDelete.cascade
                )

    :param on_update:
        Determines what the database should do when a row has it's primary key
        updated. If set to ``OnUpdate.cascade``, any rows referencing the
        updated row will have their references updated to point to the new
        primary key.

        Options:

        * ``OnUpdate.cascade`` (default)
        * ``OnUpdate.restrict``
        * ``OnUpdate.no_action``
        * ``OnUpdate.set_null``
        * ``OnUpdate.set_default``

        To learn more about the different options, see the `Postgres docs <https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-FK>`_.

        .. code-block:: python

            from piccolo.columns import OnUpdate

            class Band(Table):
                name = ForeignKey(
                    references=Manager,
                    on_update=OnUpdate.cascade
                )

    :param target_column:
        By default the ``ForeignKey`` references the primary key column on the
        related table. You can specify an alternative column (it must have a
        unique constraint on it though). For example:

        .. code-block:: python

            # Passing in a column reference:
            ForeignKey(references=Manager, target_column=Manager.passport_number)

            # Or just the column name:
            ForeignKey(references=Manager, target_column='passport_number')

    """  # noqa: E501

    _foreign_key_meta: ForeignKeyMeta

    @property
    def column_type(self):
        """
        A ``ForeignKey`` column needs to have the same type as the primary key
        column of the table being referenced.
        """
        target_column = self._foreign_key_meta.resolved_target_column

        if isinstance(target_column, BigSerial):
            return BigInt()._get_column_type(
                engine_type=self._meta.engine_type
            )
        elif isinstance(target_column, Serial):
            return Integer().column_type
        else:
            return target_column.column_type

    @property
    def value_type(self):
        """
        The value type matches that of the primary key being referenced.
        """
        target_column = self._foreign_key_meta.resolved_target_column
        return target_column.value_type

    @overload
    def __init__(
        self,
        references: type[ReferencedTable],
        default: Any = None,
        null: bool = True,
        on_delete: OnDelete = OnDelete.cascade,
        on_update: OnUpdate = OnUpdate.cascade,
        target_column: Union[str, Column, None] = None,
        **kwargs,
    ) -> None: ...

    @overload
    def __init__(
        self,
        references: LazyTableReference,
        default: Any = None,
        null: bool = True,
        on_delete: OnDelete = OnDelete.cascade,
        on_update: OnUpdate = OnUpdate.cascade,
        target_column: Union[str, Column, None] = None,
        **kwargs,
    ) -> None: ...

    @overload
    def __init__(
        self,
        references: str,
        default: Any = None,
        null: bool = True,
        on_delete: OnDelete = OnDelete.cascade,
        on_update: OnUpdate = OnUpdate.cascade,
        target_column: Union[str, Column, None] = None,
        **kwargs,
    ) -> None: ...

    def __init__(
        self,
        references: Union[type[ReferencedTable], LazyTableReference, str],
        default: Any = None,
        null: bool = True,
        on_delete: OnDelete = OnDelete.cascade,
        on_update: OnUpdate = OnUpdate.cascade,
        target_column: Union[str, Column, None] = None,
        **kwargs,
    ) -> None:
        from piccolo.table import Table

        if inspect.isclass(references):
            if issubclass(references, Table):
                # Using this to validate the default value - will raise a
                # ValueError if incorrect.
                if isinstance(references._meta.primary_key, Serial):
                    Integer(default=default, null=null)
                else:
                    references._meta.primary_key.__class__(
                        default=default, null=null
                    )

        self.default = default

        kwargs.update(
            {
                "references": references,
                "on_delete": on_delete,
                "on_update": on_update,
                "null": null,
                "target_column": target_column,
            }
        )

        super().__init__(**kwargs)

        # The ``TableMetaclass``` sets the actual value for
        # ``ForeignKeyMeta.references``, if the user passed in a string.
        self._foreign_key_meta = ForeignKeyMeta(
            references=Table if isinstance(references, str) else references,
            on_delete=on_delete,
            on_update=on_update,
            target_column=target_column,
        )

    def _setup(self, table_class: type[Table]) -> ForeignKeySetupResponse:
        """
        This is called by the ``TableMetaclass``. A ``ForeignKey`` column can
        only be completely setup once it's parent ``Table`` is known.

        :param table_class:
            The parent ``Table`` class for this column.

        """
        from piccolo.table import Table

        params = self._meta.params
        references = params["references"]

        if isinstance(references, str):
            if references == "self":
                references = table_class
            else:
                if "." in references:
                    # Don't allow relative modules - this may change in
                    # the future.
                    if references.startswith("."):
                        raise ValueError("Relative imports aren't allowed")

                    module_path, table_class_name = references.rsplit(
                        ".", maxsplit=1
                    )
                else:
                    table_class_name = references
                    module_path = table_class.__module__

                references = LazyTableReference(
                    table_class_name=table_class_name,
                    module_path=module_path,
                )

        is_lazy = isinstance(references, LazyTableReference)
        is_table_class = inspect.isclass(references) and issubclass(
            references, Table
        )

        if is_lazy or is_table_class:
            self._foreign_key_meta.references = references
        else:
            raise ValueError(
                "Error - ``references`` must be a ``Table`` subclass, or "
                "a ``LazyTableReference`` instance."
            )

        if is_table_class:
            # Record the reverse relationship on the target table.
            cast(type[Table], references)._meta._foreign_key_references.append(
                self
            )

            # Allow columns on the referenced table to be accessed via
            # auto completion.
            self.set_proxy_columns()

        return ForeignKeySetupResponse(is_lazy=is_lazy)

    def copy(self) -> ForeignKey:
        column: ForeignKey = copy.copy(self)
        column._meta = self._meta.copy()
        column._foreign_key_meta = self._foreign_key_meta.copy()
        return column

    def all_columns(
        self, exclude: Optional[list[Union[Column, str]]] = None
    ) -> list[Column]:
        """
        Allow a user to access all of the columns on the related table. This is
        intended for use with ``select`` queries, and saves the user from
        typing out all of the columns by hand.

        For example:

        .. code-block:: python

            await Band.select(Band.name, Band.manager.all_columns())

            # Equivalent to:
            await Band.select(
                Band.name,
                Band.manager.id,
                Band.manager.name
            )

        To exclude certain columns:

        .. code-block:: python

            await Band.select(
                Band.name,
                Band.manager.all_columns(
                    exclude=[Band.manager.id]
                )
            )

        :param exclude:
            Columns to exclude - can be the name of a column, or a column
            instance. For example ``['id']`` or ``[Band.manager.id]``.

        """
        if exclude is None:
            exclude = []
        _fk_meta = object.__getattribute__(self, "_foreign_key_meta")

        excluded_column_names = [
            i._meta.name if isinstance(i, Column) else i for i in exclude
        ]

        return [
            getattr(self, column._meta.name)
            for column in _fk_meta.resolved_references._meta.columns
            if column._meta.name not in excluded_column_names
        ]

    def reverse(self) -> ForeignKey:
        """
        If there's a unique foreign key, this function reverses it.

        .. code-block:: python

            class Band(Table):
                name = Varchar()

            class FanClub(Table):
                band = ForeignKey(Band, unique=True)
                address = Text()

            class Treasurer(Table):
                fan_club = ForeignKey(FanClub, unique=True)
                name = Varchar()

        It's helpful with ``get_related``, for example:

        .. code-block:: python

            >>> band = await Band.objects().first()
            >>> await band.get_related(FanClub.band.reverse())
            <Fan Club: 1>

        It works multiple levels deep:

        .. code-block:: python

            >>> await band.get_related(Treasurer.fan_club._.band.reverse())
            <Treasurer: 1>

        """
        if not self._meta.unique or any(
            not i._meta.unique for i in self._meta.call_chain
        ):
            raise ValueError("Only reverse unique foreign keys.")

        foreign_keys = [*self._meta.call_chain, self]

        root_foreign_key = foreign_keys[0]
        target_column = (
            root_foreign_key._foreign_key_meta.resolved_target_column
        )
        foreign_key = target_column.join_on(root_foreign_key)

        call_chain = []
        for fk in reversed(foreign_keys[1:]):
            target_column = fk._foreign_key_meta.resolved_target_column
            call_chain.append(target_column.join_on(fk))

        foreign_key._meta.call_chain = call_chain

        return foreign_key

    def all_related(
        self, exclude: Optional[list[Union[ForeignKey, str]]] = None
    ) -> list[ForeignKey]:
        """
        Returns each ``ForeignKey`` column on the related table. This is
        intended for use with ``objects`` queries, where you want to return
        all of the related tables as nested objects.

        For example:

        .. code-block:: python

            class Band(Table):
                name = Varchar()

            class Concert(Table):
                name = Varchar()
                band_1 = ForeignKey(Band)
                band_2 = ForeignKey(Band)

            class Tour(Table):
                name = Varchar()
                concert = ForeignKey(Concert)

            await Tour.objects(Tour.concert, Tour.concert.all_related())

            # Equivalent to
            await Tour.objects(
                Tour.concert,
                Tour.concert.band_1,
                Tour.concert.band_2
            )

        :param exclude:
            Columns to exclude - can be the name of a column, or a
            ``ForeignKey`` instance. For example ``['band_1']`` or
            ``[Tour.concert.band_1]``.

        """
        if exclude is None:
            exclude = []
        _fk_meta: ForeignKeyMeta = object.__getattribute__(
            self, "_foreign_key_meta"
        )
        related_fk_columns = (
            _fk_meta.resolved_references._meta.foreign_key_columns
        )
        excluded_column_names = [
            i._meta.name if isinstance(i, ForeignKey) else i for i in exclude
        ]
        return [
            getattr(self, fk_column._meta.name)
            for fk_column in related_fk_columns
            if fk_column._meta.name not in excluded_column_names
        ]

    def set_proxy_columns(self) -> None:
        """
        In order to allow a fluent interface, where tables can be traversed
        using ForeignKeys (e.g. ``Band.manager.name``), we add attributes to
        the ``ForeignKey`` column for each column in the table being pointed
        to.
        """
        _fk_meta = object.__getattribute__(self, "_foreign_key_meta")
        for column in _fk_meta.resolved_references._meta.columns:
            _column: Column = column.copy()
            setattr(self, _column._meta.name, _column)
            _fk_meta.proxy_columns.append(_column)

    @property
    def _(self) -> type[ReferencedTable]:
        """
        This allows us specify joins in a way which is friendly to static type
        checkers like Mypy and Pyright.

        Whilst this works::

            # Fetch the band's name, and their manager's name via a foreign
            # key:
            await Band.select(Band.name, Band.manager.name)

        There currently isn't a 100% reliable way to tell static type checkers
        that ``Band.manager.name`` refers to a ``name`` column on the
        ``Manager`` table.

        However, by using the ``_`` property, it works perfectly. Instead
        of ``Band.manager.name`` we use ``Band.manager._.name``::

            await Band.select(Band.name, Band.manager._.name)

        So when doing joins, after every foreign key we use ``._.`` instead of
        ``.``. An easy way to remember this is ``._.`` looks a bit like a
        connector in a diagram.

        As Python's typing support increases, we'd love ``Band.manager.name``
        to have the same static typing as ``Band.manager._.name`` (using some
        kind of ``Proxy`` type), but for now this is the best solution, and is
        a huge improvement in developer experience, as static type checkers
        easily know if any of your joins contain typos.

        """
        return cast(type[ReferencedTable], self)

    def __getattribute__(self, name: str) -> Union[Column, Any]:
        """
        Returns attributes unmodified unless they're Column instances, in which
        case a copy is returned with an updated call_chain (which records the
        joins required).
        """
        # If the ForeignKey is using a lazy reference, we need to set the
        # attributes here. Attributes starting with an underscore are
        # unlikely to be column names.
        if not name.startswith("_") and name not in dir(self):
            try:
                _foreign_key_meta = object.__getattribute__(
                    self, "_foreign_key_meta"
                )
            except AttributeError:
                pass
            else:
                if _foreign_key_meta.proxy_columns == [] and isinstance(
                    _foreign_key_meta.references, LazyTableReference
                ):
                    object.__getattribute__(self, "set_proxy_columns")()

        value = object.__getattribute__(self, name)

        if name.startswith("_"):
            return value

        foreignkey_class: type[ForeignKey] = object.__getattribute__(
            self, "__class__"
        )

        if isinstance(value, foreignkey_class):  # i.e. a ForeignKey
            new_column = value.copy()
            new_column._meta.call_chain.append(self)

            # We have to set limits to the call chain because Table 1 can
            # reference Table 2, which references Table 1, creating an endless
            # loop. For now an arbitrary limit is set of 10 levels deep.
            # When querying a call chain more than 10 levels deep, an error
            # will be raised. Often there are more effective ways of
            # structuring a query than joining so many tables anyway.
            if len(new_column._meta.call_chain) >= 10:
                raise Exception("Call chain too long!")

            foreign_key_meta: ForeignKeyMeta = object.__getattribute__(
                new_column, "_foreign_key_meta"
            )

            for proxy_column in foreign_key_meta.proxy_columns:
                try:
                    delattr(new_column, proxy_column._meta.name)
                except Exception:
                    pass

            foreign_key_meta.proxy_columns = []

            for (
                column
            ) in value._foreign_key_meta.resolved_references._meta.columns:
                _column: Column = column.copy()
                _column._meta.call_chain = list(new_column._meta.call_chain)
                setattr(new_column, _column._meta.name, _column)
                foreign_key_meta.proxy_columns.append(_column)

            return new_column
        elif issubclass(type(value), Column):
            new_column = value.copy()

            column_meta: ColumnMeta = object.__getattribute__(self, "_meta")

            new_column._meta.call_chain = column_meta.call_chain.copy()

            new_column._meta.call_chain.append(self)
            return new_column
        else:
            return value

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> Any: ...

    @overload
    def __get__(
        self, obj: None, objtype=None
    ) -> ForeignKey[ReferencedTable]: ...

    @overload
    def __get__(self, obj: Any, objtype=None) -> Any: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Any):
        obj.__dict__[self._meta.name] = value


###############################################################################


class JSON(Column):
    """
    Used for storing JSON strings. The data is stored as text. This can be
    preferable to JSONB if you just want to store and retrieve JSON without
    querying it directly. It works with SQLite and Postgres.

    :param default:
        Either a JSON string can be provided, or a Python ``dict`` or ``list``
        which is then converted to a JSON string.

    """

    value_type = str

    def __init__(
        self,
        default: Union[
            str,
            list,
            dict,
            Callable[[], Union[str, list, dict]],
            None,
        ] = "{}",
        **kwargs: Unpack[ColumnKwargs],
    ) -> None:
        self._validate_default(default, (str, list, dict, None))

        if isinstance(default, (list, dict)):
            default = dump_json(default)

        self.default = default
        super().__init__(default=default, **kwargs)

        self.json_operator: Optional[str] = None

    @property
    def column_type(self):
        engine_type = self._meta.engine_type
        if engine_type == "cockroach":
            return "JSONB"  # Cockroach is always JSONB.
        else:
            return "JSON"

    ###########################################################################

    def arrow(self, key: Union[str, int, QueryString]) -> GetChildElement:
        """
        Allows a child element of the JSON structure to be returned - for
        example::

            >>> await RecordingStudio.select(
            ...     RecordingStudio.facilities.arrow("restaurant")
            ... )

        """
        from piccolo.query.operators.json import GetChildElement

        alias = self._alias or self._meta.get_default_alias()
        return GetChildElement(identifier=self, key=key, alias=alias)

    def __getitem__(
        self, value: Union[str, int, QueryString]
    ) -> GetChildElement:
        """
        A shortcut for the ``arrow`` method, used for retrieving a child
        element.

        For example:

        .. code-block:: python

            >>> await RecordingStudio.select(
            ...     RecordingStudio.facilities["restaurant"]
            ... )

        """
        return self.arrow(key=value)

    def from_path(
        self,
        path: list[Union[str, int]],
    ) -> GetElementFromPath:
        """
        Allows an element of the JSON structure to be returned, which can be
        arbitrarily deep. For example::

            >>> await RecordingStudio.select(
            ...     RecordingStudio.facilities.from_path([
            ...         "technician",
            ...         0,
            ...         "first_name"
            ...     ])
            ... )

        It's the same as calling ``arrow`` multiple times, but is more
        efficient / convenient if extracting highly nested data::

            >>> await RecordingStudio.select(
            ...     RecordingStudio.facilities.arrow(
            ...         "technician"
            ...     ).arrow(
            ...         0
            ...     ).arrow(
            ...         "first_name"
            ...     )
            ... )

        """
        from piccolo.query.operators.json import GetElementFromPath

        alias = self._alias or self._meta.get_default_alias()
        return GetElementFromPath(identifier=self, path=path, alias=alias)

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> str: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> JSON: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[str, dict]):
        obj.__dict__[self._meta.name] = value


class JSONB(JSON):
    """
    Used for storing JSON strings - Postgres / CochroachDB only. The data is
    stored in a binary format, and can be queried more efficiently. Insertion
    can be slower (as it needs to be converted to the binary format). The
    benefits of JSONB generally outweigh the downsides.

    :param default:
        Either a JSON string can be provided, or a Python ``dict`` or ``list``
        which is then converted to a JSON string.

    """

    @property
    def column_type(self):
        return "JSONB"  # Must be defined, we override column_type() in JSON()

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> str: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> JSONB: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: Union[str, dict]):
        obj.__dict__[self._meta.name] = value


###############################################################################


class Bytea(Column):
    """
    Used for storing bytes.

    **Example**

    .. code-block:: python

        class Token(Table):
            token = Bytea(default=b'token123')

        # Create
        >>> await Token(token=b'my-token').save()

        # Query
        >>> await Token.select(Token.token)
        {'token': b'my-token'}

    """

    value_type = bytes

    @property
    def column_type(self):
        engine_type = self._meta.engine_type
        if engine_type in ("postgres", "cockroach"):
            return "BYTEA"
        elif engine_type == "sqlite":
            return "BLOB"
        raise Exception("Unrecognized engine type")

    def __init__(
        self,
        default: Union[
            bytes,
            bytearray,
            Enum,
            Callable[[], bytes],
            Callable[[], bytearray],
            None,
        ] = b"",
        **kwargs: Unpack[ColumnKwargs],
    ) -> None:
        self._validate_default(default, (bytes, bytearray, None))

        if isinstance(default, bytearray):
            default = bytes(default)

        self.default = default
        super().__init__(default=default, **kwargs)

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> bytes: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> Bytea: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: bytes):
        obj.__dict__[self._meta.name] = value


class Blob(Bytea):
    """
    An alias for Bytea.
    """

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> bytes: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> Blob: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: bytes):
        obj.__dict__[self._meta.name] = value


###############################################################################


class ListProxy:
    """
    Sphinx's autodoc fails if we have this function signature::

        class Array(Column):

            def __init__(default=list):
                ...

    We can't use ``list`` as a default value without breaking autodoc (it
    doesn't seem to like it when a class type is used as a default), so
    instead we assign an instance of this class. It keeps both autodoc and MyPy
    happy. In ``Array.__init__`` we then swap it out for ``list``.
    """

    def __call__(self):
        return []

    def __repr__(self):
        return "list"


class Array(Column):
    """
    Used for storing lists of data.

    **Example**

    .. code-block:: python

        class Ticket(Table):
            seat_numbers = Array(base_column=Integer())

        # Create
        >>> await Ticket(seat_numbers=[34, 35, 36]).save()

        # Query
        >>> await Ticket.select(Ticket.seat_numbers)
        {'seat_numbers': [34, 35, 36]}

    """

    value_type = list

    def __init__(
        self,
        base_column: Column,
        default: Union[list, Enum, Callable[[], list], None] = ListProxy(),
        **kwargs: Unpack[ColumnKwargs],
    ) -> None:
        if isinstance(base_column, ForeignKey):
            raise ValueError("Arrays of ForeignKeys aren't allowed.")

        # This is a workaround because having `list` as a default breaks
        # Sphinx's autodoc.
        if isinstance(default, ListProxy):
            default = list

        self._validate_default(default, (list, None))

        choices = kwargs.get("choices")
        if choices is not None:
            self._validate_choices(
                choices, allowed_type=base_column.value_type
            )
            self._validated_choices = True

        # Usually columns are given a name by the Table metaclass, but in this
        # case we have to assign one manually to the base column.
        base_column._meta._name = base_column.__class__.__name__

        self.base_column = base_column
        self.default = default
        self.index: Optional[int] = None
        super().__init__(default=default, base_column=base_column, **kwargs)

    @property
    def column_type(self):
        engine_type = self._meta.engine_type
        if engine_type in ("postgres", "cockroach"):
            return f"{self.base_column.column_type}[]"
        elif engine_type == "sqlite":
            inner_column = self._get_inner_column()
            return (
                f"ARRAY_{inner_column.column_type}"
                if isinstance(
                    inner_column, (Date, Timestamp, Timestamptz, Time)
                )
                else "ARRAY"
            )
        raise Exception("Unrecognized engine type")

    def _setup_base_column(self, table_class: type[Table]):
        """
        Called from the ``Table.__init_subclass__`` - makes sure
        that the ``base_column`` has a reference to the parent table.
        """
        self.base_column._meta._table = table_class
        if isinstance(self.base_column, Array):
            self.base_column._setup_base_column(table_class=table_class)

    def _get_dimensions(self, start: int = 0) -> int:
        """
        A helper function to get the number of dimensions for the array. For
        example::

            >>> Array(Varchar())._get_dimensions()
            1

            >>> Array(Array(Varchar()))._get_dimensions()
            2

        :param start:
            Ignore this - it's just used for  calling this method recursively.

        """
        if isinstance(self.base_column, Array):
            return self.base_column._get_dimensions(start=start + 1)
        else:
            return start + 1

    def _get_inner_column(self) -> Column:
        """
        A helper function to get the innermost ``Column`` for the array. For
        example::

            >>> Array(Varchar())._get_inner_column()
            Varchar

            >>> Array(Array(Varchar()))._get_inner_column()
            Varchar

        """
        if isinstance(self.base_column, Array):
            return self.base_column._get_inner_column()
        else:
            return self.base_column

    def _get_inner_value_type(self) -> type:
        """
        A helper function to get the innermost value type for the array. For
        example::

            >>> Array(Varchar())._get_inner_value_type()
            str

            >>> Array(Array(Varchar()))._get_inner_value_type()
            str

        """
        return self._get_inner_column().value_type

    def __getitem__(self, value: int) -> Array:
        """
        Allows queries which retrieve an item from the array. The index starts
        with 0 for the first value. If you were to write the SQL by hand, the
        first index would be 1 instead (see `Postgres array docs <https://www.postgresql.org/docs/current/arrays.html>`_).

        However, we keep the first index as 0 to fit better with Python.

        For example:

        .. code-block:: python

            >>> await Ticket.select(Ticket.seat_numbers[0]).first()
            {'seat_numbers': 325}


        """  # noqa: E501
        engine_type = self._meta.engine_type
        if engine_type != "postgres" and engine_type != "cockroach":
            raise ValueError(
                "Only Postgres and Cockroach support array indexing."
            )

        if isinstance(value, int):
            if value < 0:
                raise ValueError("Only positive integers are allowed.")

            instance = cast(Array, self.copy())

            # We deliberately add 1, as Postgres treats the first array element
            # as index 1.
            instance.index = value + 1
            return instance
        else:
            raise ValueError("Only integers can be used for indexing.")

    def get_select_string(
        self, engine_type: str, with_alias=True
    ) -> QueryString:
        select_string = self._meta.get_full_name(with_alias=False)

        if isinstance(self.index, int):
            select_string += f"[{self.index}]"

        if with_alias:
            alias = self._alias or self._meta.get_default_alias()
            select_string += f' AS "{alias}"'

        return QueryString(select_string)

    def any(self, value: Any) -> Where:
        """
        Check if any of the items in the array match the given value.

        .. code-block:: python

            >>> await Ticket.select().where(Ticket.seat_numbers.any(510))

        """
        engine_type = self._meta.engine_type

        if engine_type in ("postgres", "cockroach"):
            return Where(column=self, value=value, operator=ArrayAny)
        elif engine_type == "sqlite":
            return self.like(f"%{value}%")
        else:
            raise ValueError("Unrecognised engine type")

    def not_any(self, value: Any) -> Where:
        """
        Check if the given value isn't in the array.

        .. code-block:: python

            >>> await Ticket.select().where(Ticket.seat_numbers.not_any(510))

        """
        engine_type = self._meta.engine_type

        if engine_type in ("postgres", "cockroach"):
            return Where(column=self, value=value, operator=ArrayNotAny)
        elif engine_type == "sqlite":
            return self.not_like(f"%{value}%")
        else:
            raise ValueError("Unrecognised engine type")

    def all(self, value: Any) -> Where:
        """
        Check if all of the items in the array match the given value.

        .. code-block:: python

            >>> await Ticket.select().where(Ticket.seat_numbers.all(510))

        """
        engine_type = self._meta.engine_type

        if engine_type in ("postgres", "cockroach"):
            return Where(column=self, value=value, operator=ArrayAll)
        elif engine_type == "sqlite":
            raise ValueError("Unsupported by SQLite")
        else:
            raise ValueError("Unrecognised engine type")

    def cat(self, value: ArrayType) -> QueryString:
        """
        A convenient way of accessing the
        :class:`ArrayCat <piccolo.query.functions.array.ArrayCat>` function.

        Used in an ``update`` query to concatenate two arrays.

        .. code-block:: python

            >>> await Ticket.update({
            ...     Ticket.seat_numbers: Ticket.seat_numbers.cat([1000])
            ... }).where(Ticket.id == 1)

        You can also use the ``+`` symbol if you prefer. To concatenate to
        the end:

        .. code-block:: python

            >>> await Ticket.update({
            ...     Ticket.seat_numbers: Ticket.seat_numbers + [1000]
            ... }).where(Ticket.id == 1)

        To concatenate to the start:

        .. code-block:: python

            >>> await Ticket.update({
            ...     Ticket.seat_numbers: [1000] + Ticket.seat_numbers
            ... }).where(Ticket.id == 1)

        You can concatenate multiple arrays in one go:

        .. code-block:: python

            >>> await Ticket.update({
            ...     Ticket.seat_numbers: [1000] + Ticket.seat_numbers + [2000]
            ... }).where(Ticket.id == 1)

        .. note:: Postgres / CockroachDB only

        """
        from piccolo.query.functions.array import ArrayCat

        # Keep this for backwards compatibility - we had this as a convenience
        # for users, but it would be nice to remove it in the future.
        if not isinstance(value, list):
            value = [value]

        return ArrayCat(array_1=self, array_2=value)

    def remove(self, value: ArrayItemType) -> QueryString:
        """
        A convenient way of accessing the
        :class:`ArrayRemove <piccolo.query.functions.array.ArrayRemove>`
        function.

        Used in an ``update`` query to remove an item from an array.

        .. code-block:: python

            >>> await Ticket.update({
            ...     Ticket.seat_numbers: Ticket.seat_numbers.remove(1000)
            ... }).where(Ticket.id == 1)

        .. note:: Postgres / CockroachDB only

        """
        from piccolo.query.functions.array import ArrayRemove

        return ArrayRemove(array=self, value=value)

    def prepend(self, value: ArrayItemType) -> QueryString:
        """
        A convenient way of accessing the
        :class:`ArrayPrepend <piccolo.query.functions.array.ArrayPrepend>`
        function.

        Used in an ``update`` query to prepend an item to an array.

        .. code-block:: python

            >>> await Ticket.update({
            ...     Ticket.seat_numbers: Ticket.seat_numbers.prepend(1000)
            ... }).where(Ticket.id == 1)

        .. note:: Postgres / CockroachDB only

        """
        from piccolo.query.functions.array import ArrayPrepend

        return ArrayPrepend(array=self, value=value)

    def append(self, value: ArrayItemType) -> QueryString:
        """
        A convenient way of accessing the
        :class:`ArrayAppend <piccolo.query.functions.array.ArrayAppend>`
        function.

        Used in an ``update`` query to append an item to an array.

        .. code-block:: python

            >>> await Ticket.update({
            ...     Ticket.seat_numbers: Ticket.seat_numbers.append(1000)
            ... }).where(Ticket.id == 1)

        .. note:: Postgres / CockroachDB only

        """
        from piccolo.query.functions.array import ArrayAppend

        return ArrayAppend(array=self, value=value)

    def replace(
        self, old_value: ArrayItemType, new_value: ArrayItemType
    ) -> QueryString:
        """
        A convenient way of accessing the
        :class:`ArrayReplace <piccolo.query.functions.array.ArrayReplace>`
        function.

        Used in an ``update`` query to replace each array item
        equal to the given value with a new value.

        .. code-block:: python

            >>> await Ticket.update({
            ...     Ticket.seat_numbers: Ticket.seat_numbers.replace(1000, 500)
            ... }).where(Ticket.id == 1)

        .. note:: Postgres / CockroachDB only

        """
        from piccolo.query.functions.array import ArrayReplace

        return ArrayReplace(self, old_value=old_value, new_value=new_value)

    def __add__(self, value: ArrayType) -> QueryString:
        return self.cat(value)

    def __radd__(self, value: ArrayType) -> QueryString:
        from piccolo.query.functions.array import ArrayCat

        return ArrayCat(array_1=value, array_2=self)

    ###########################################################################
    # Descriptors

    @overload
    def __get__(self, obj: Table, objtype=None) -> list[Any]: ...

    @overload
    def __get__(self, obj: None, objtype=None) -> Array: ...

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self._meta.name] if obj else self

    def __set__(self, obj, value: list[Any]):
        obj.__dict__[self._meta.name] = value
