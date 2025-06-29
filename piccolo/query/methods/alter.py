from __future__ import annotations

import itertools
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, TypeVar, Union

from piccolo.columns.base import Column
from piccolo.columns.column_types import ForeignKey, Numeric, Varchar
from piccolo.query.base import DDL
from piccolo.utils.warnings import Level, colored_warning

if TYPE_CHECKING:  # pragma: no cover
    from piccolo.columns.base import OnDelete, OnUpdate
    from piccolo.table import Table


class AlterStatement:
    __slots__ = ()  # type: ignore

    @property
    def ddl(self) -> str:
        raise NotImplementedError()

    def __str__(self) -> str:
        return self.ddl


@dataclass
class RenameTable(AlterStatement):
    __slots__ = ("new_name",)

    new_name: str

    @property
    def ddl(self) -> str:
        return f"RENAME TO {self.new_name}"


@dataclass
class RenameConstraint(AlterStatement):
    __slots__ = ("old_name", "new_name")

    old_name: str
    new_name: str

    @property
    def ddl(self) -> str:
        return f"RENAME CONSTRAINT {self.old_name} TO {self.new_name}"


@dataclass
class AlterColumnStatement(AlterStatement):
    __slots__ = ("column",)

    column: Union[Column, str]

    @property
    def column_name(self) -> str:
        if isinstance(self.column, str):
            return self.column
        elif isinstance(self.column, Column):
            return self.column._meta.db_column_name
        else:
            raise ValueError("Unrecognised column type")


@dataclass
class RenameColumn(AlterColumnStatement):
    __slots__ = ("new_name",)

    new_name: str

    @property
    def ddl(self) -> str:
        return f'RENAME COLUMN "{self.column_name}" TO "{self.new_name}"'


@dataclass
class DropColumn(AlterColumnStatement):
    @property
    def ddl(self) -> str:
        return f'DROP COLUMN "{self.column_name}"'


@dataclass
class AddColumn(AlterColumnStatement):
    __slots__ = ("name",)

    column: Column
    name: str

    @property
    def ddl(self) -> str:
        self.column._meta.name = self.name
        return f"ADD COLUMN {self.column.ddl}"


@dataclass
class DropDefault(AlterColumnStatement):
    @property
    def ddl(self) -> str:
        return f'ALTER COLUMN "{self.column_name}" DROP DEFAULT'


@dataclass
class SetColumnType(AlterStatement):
    """
    :param using_expression:
        Postgres can't automatically convert between certain column types. You
        can tell Postgres which action to take. For example
        `my_column_name::integer`.

    """

    old_column: Column
    new_column: Column
    using_expression: Optional[str] = None

    @property
    def ddl(self) -> str:
        if self.new_column._meta._table is None:
            self.new_column._meta._table = self.old_column._meta.table

        column_name = self.old_column._meta.db_column_name
        query = (
            f'ALTER COLUMN "{column_name}" TYPE {self.new_column.column_type}'
        )
        if self.using_expression is not None:
            query += f" USING {self.using_expression}"
        return query


@dataclass
class SetDefault(AlterColumnStatement):
    __slots__ = ("value",)

    column: Column
    value: Any

    @property
    def ddl(self) -> str:
        sql_value = self.column.get_sql_value(self.value)
        return f'ALTER COLUMN "{self.column_name}" SET DEFAULT {sql_value}'


@dataclass
class SetUnique(AlterColumnStatement):
    __slots__ = ("boolean",)

    boolean: bool

    @property
    def ddl(self) -> str:
        if self.boolean:
            return f'ADD UNIQUE ("{self.column_name}")'
        if isinstance(self.column, str):
            raise ValueError(
                "Removing a unique constraint requires a Column instance "
                "to be passed as the column arg instead of a string."
            )
        tablename = self.column._meta.table._meta.tablename
        column_name = self.column_name
        key = f"{tablename}_{column_name}_key"
        return f'DROP CONSTRAINT "{key}"'


@dataclass
class SetNull(AlterColumnStatement):
    __slots__ = ("boolean",)

    boolean: bool

    @property
    def ddl(self) -> str:
        if self.boolean:
            return f'ALTER COLUMN "{self.column_name}" DROP NOT NULL'
        else:
            return f'ALTER COLUMN "{self.column_name}" SET NOT NULL'


@dataclass
class SetLength(AlterColumnStatement):
    __slots__ = ("length",)

    length: int

    @property
    def ddl(self) -> str:
        return f'ALTER COLUMN "{self.column_name}" TYPE VARCHAR({self.length})'


@dataclass
class DropConstraint(AlterStatement):
    __slots__ = ("constraint_name",)

    constraint_name: str

    @property
    def ddl(self) -> str:
        return f"DROP CONSTRAINT IF EXISTS {self.constraint_name}"


@dataclass
class AddForeignKeyConstraint(AlterStatement):
    __slots__ = (
        "constraint_name",
        "foreign_key_column_name",
        "referenced_table_name",
        "referenced_column_name",
        "on_delete",
        "on_update",
    )

    constraint_name: str
    foreign_key_column_name: str
    referenced_table_name: str
    referenced_column_name: str
    on_delete: Optional[OnDelete]
    on_update: Optional[OnUpdate]

    @property
    def ddl(self) -> str:
        query = (
            f'ADD CONSTRAINT "{self.constraint_name}" FOREIGN KEY '
            f'("{self.foreign_key_column_name}") REFERENCES '
            f'"{self.referenced_table_name}" ("{self.referenced_column_name}")'
        )
        if self.on_delete:
            query += f" ON DELETE {self.on_delete.value}"
        if self.on_update:
            query += f" ON UPDATE {self.on_update.value}"
        return query


@dataclass
class SetDigits(AlterColumnStatement):
    __slots__ = ("digits", "column_type")

    digits: Optional[tuple[int, int]]
    column_type: str

    @property
    def ddl(self) -> str:
        if self.digits is None:
            return f'ALTER COLUMN "{self.column_name}" TYPE {self.column_type}'

        precision = self.digits[0]
        scale = self.digits[1]
        return (
            f'ALTER COLUMN "{self.column_name}" TYPE '
            f"{self.column_type}({precision}, {scale})"
        )


@dataclass
class SetSchema(AlterStatement):
    __slots__ = ("schema_name",)

    schema_name: str

    @property
    def ddl(self) -> str:
        return f'SET SCHEMA "{self.schema_name}"'


@dataclass
class DropTable:
    table: type[Table]
    cascade: bool
    if_exists: bool

    @property
    def ddl(self) -> str:
        query = "DROP TABLE"

        if self.if_exists:
            query += " IF EXISTS"

        query += f" {self.table._meta.get_formatted_tablename()}"

        if self.cascade:
            query += " CASCADE"

        return query


class Alter(DDL):
    __slots__ = (
        "_add",
        "_add_foreign_key_constraint",
        "_drop_constraint",
        "_drop_default",
        "_drop_table",
        "_drop",
        "_rename_columns",
        "_rename_table",
        "_set_column_type",
        "_set_default",
        "_set_digits",
        "_set_length",
        "_set_null",
        "_set_schema",
        "_set_unique",
        "_rename_constraint",
    )

    def __init__(self, table: type[Table], **kwargs):
        super().__init__(table, **kwargs)
        self._add_foreign_key_constraint: list[AddForeignKeyConstraint] = []
        self._add: list[AddColumn] = []
        self._drop_constraint: list[DropConstraint] = []
        self._drop_default: list[DropDefault] = []
        self._drop_table: Optional[DropTable] = None
        self._drop: list[DropColumn] = []
        self._rename_columns: list[RenameColumn] = []
        self._rename_table: list[RenameTable] = []
        self._set_column_type: list[SetColumnType] = []
        self._set_default: list[SetDefault] = []
        self._set_digits: list[SetDigits] = []
        self._set_length: list[SetLength] = []
        self._set_null: list[SetNull] = []
        self._set_schema: list[SetSchema] = []
        self._set_unique: list[SetUnique] = []
        self._rename_constraint: list[RenameConstraint] = []

    def add_column(self: Self, name: str, column: Column) -> Self:
        """
        Add a column to the table::

            >>> await Band.alter().add_column('members', Integer())

        """
        column._meta._table = self.table
        column._meta._name = name
        column._meta.db_column_name = name

        if isinstance(column, ForeignKey):
            column._setup(table_class=self.table)

        self._add.append(AddColumn(column, name))
        return self

    def drop_column(self, column: Union[str, Column]) -> Alter:
        """
        Drop a column from the table::

            >>> await Band.alter().drop_column(Band.popularity)

        """
        self._drop.append(DropColumn(column))
        return self

    def drop_default(self, column: Union[str, Column]) -> Alter:
        """
        Drop the default from a column::

            >>> await Band.alter().drop_default(Band.popularity)

        """
        self._drop_default.append(DropDefault(column=column))
        return self

    def drop_table(
        self, cascade: bool = False, if_exists: bool = False
    ) -> Alter:
        """
        Drop the table::

            >>> await Band.alter().drop_table()

        """
        self._drop_table = DropTable(
            table=self.table,
            cascade=cascade,
            if_exists=if_exists,
        )
        return self

    def rename_table(self, new_name: str) -> Alter:
        """
        Rename the table::

            >>> await Band.alter().rename_table('musical_group')

        """
        # We override the existing one rather than appending.
        self._rename_table = [RenameTable(new_name=new_name)]
        return self

    def rename_constraint(self, old_name: str, new_name: str) -> Alter:
        """
        Rename a constraint on the table::

            >>> await Band.alter().rename_constraint(
            ...     'old_constraint_name',
            ...     'new_constraint_name',
            ... )

        """
        self._rename_constraint = [
            RenameConstraint(
                old_name=old_name,
                new_name=new_name,
            )
        ]
        return self

    def rename_column(
        self, column: Union[str, Column], new_name: str
    ) -> Alter:
        """
        Rename a column on the table::

            # Specify the column with a `Column` instance:
            >>> await Band.alter().rename_column(Band.popularity, 'rating')

            # Or by name:
            >>> await Band.alter().rename_column('popularity', 'rating')

        """
        self._rename_columns.append(RenameColumn(column, new_name))
        return self

    def set_column_type(
        self,
        old_column: Column,
        new_column: Column,
        using_expression: Optional[str] = None,
    ) -> Alter:
        """
        Change the type of a column::

            >>> await Band.alter().set_column_type(Band.popularity, BigInt())

        :param using_expression:
            When changing a column's type, the database doesn't always know how
            to convert the existing data in that column to the new type. You
            can provide a hint to the database on what to do. For example
            ``'name::integer'``.

        """
        self._set_column_type.append(
            SetColumnType(
                old_column=old_column,
                new_column=new_column,
                using_expression=using_expression,
            )
        )
        return self

    def set_default(self, column: Column, value: Any) -> Alter:
        """
        Set the default for a column::

            >>> await Band.alter().set_default(Band.popularity, 0)

        """
        self._set_default.append(SetDefault(column=column, value=value))
        return self

    def set_null(
        self, column: Union[str, Column], boolean: bool = True
    ) -> Alter:
        """
        Change a column to be nullable or not::

            # Specify the column using a `Column` instance:
            >>> await Band.alter().set_null(Band.name, True)

            # Or using a string:
            >>> await Band.alter().set_null('name', True)

        """
        self._set_null.append(SetNull(column, boolean))
        return self

    def set_unique(
        self, column: Union[str, Column], boolean: bool = True
    ) -> Alter:
        """
        Make a column unique or not::

            # Specify the column using a `Column` instance:
            >>> await Band.alter().set_unique(Band.name, True)

            # Or using a string:
            >>> await Band.alter().set_unique('name', True)

        """
        self._set_unique.append(SetUnique(column, boolean))
        return self

    def set_length(self, column: Union[str, Varchar], length: int) -> Alter:
        """
        Change the max length of a varchar column. Unfortunately, this isn't
        supported by SQLite, but SQLite also doesn't enforce any length limits
        on varchar columns anyway::

            >>> await Band.alter().set_length('name', 512)

        """
        if self.engine_type == "sqlite":
            colored_warning(
                (
                    "SQLITE doesn't support changes in length. It also "
                    "doesn't enforce any length limits, so your code will "
                    "still work as expected. Skipping."
                ),
                level=Level.medium,
            )
            return self

        if not isinstance(column, (str, Varchar)):
            raise ValueError(
                "Only Varchar columns can have their length changed."
            )

        self._set_length.append(SetLength(column, length))
        return self

    def _get_constraint_name(self, column: Union[str, ForeignKey]) -> str:
        column_name = AlterColumnStatement(column=column).column_name
        tablename = self.table._meta.tablename
        return f"{tablename}_{column_name}_fkey"

    def drop_constraint(self, constraint_name: str) -> Alter:
        self._drop_constraint.append(
            DropConstraint(constraint_name=constraint_name)
        )
        return self

    def drop_foreign_key_constraint(
        self, column: Union[str, ForeignKey]
    ) -> Alter:
        constraint_name = self._get_constraint_name(column=column)
        self._drop_constraint.append(
            DropConstraint(constraint_name=constraint_name)
        )
        return self

    def add_foreign_key_constraint(
        self,
        column: Union[str, ForeignKey],
        referenced_table_name: Optional[str] = None,
        referenced_column_name: Optional[str] = None,
        constraint_name: Optional[str] = None,
        on_delete: Optional[OnDelete] = None,
        on_update: Optional[OnUpdate] = None,
    ) -> Alter:
        """
        Add a new foreign key constraint::

            >>> await Band.alter().add_foreign_key_constraint(
            ...     Band.manager,
            ...     on_delete=OnDelete.cascade
            ... )

        """
        constraint_name = constraint_name or self._get_constraint_name(
            column=column
        )
        column_name = AlterColumnStatement(column=column).column_name

        if referenced_column_name is None:
            if isinstance(column, ForeignKey):
                referenced_column_name = (
                    column._foreign_key_meta.resolved_target_column._meta.db_column_name  # noqa: E501
                )
            else:
                raise ValueError("Please pass in `referenced_column_name`.")

        if referenced_table_name is None:
            if isinstance(column, ForeignKey):
                referenced_table_name = (
                    column._foreign_key_meta.resolved_references._meta.tablename  # noqa: E501
                )
            else:
                raise ValueError("Please pass in `referenced_table_name`.")

        self._add_foreign_key_constraint.append(
            AddForeignKeyConstraint(
                constraint_name=constraint_name,
                foreign_key_column_name=column_name,
                referenced_table_name=referenced_table_name,
                referenced_column_name=referenced_column_name,
                on_delete=on_delete,
                on_update=on_update,
            )
        )
        return self

    def set_digits(
        self,
        column: Union[str, Numeric],
        digits: Optional[tuple[int, int]],
    ) -> Alter:
        """
        Alter the precision and scale for a ``Numeric`` column.
        """
        column_type = (
            column.__class__.__name__.upper()
            if isinstance(column, Numeric)
            else "NUMERIC"
        )
        self._set_digits.append(
            SetDigits(
                digits=digits,
                column=column,
                column_type=column_type,
            )
        )
        return self

    def set_schema(self, schema_name: str) -> Alter:
        """
        Move the table to a different schema.

        :param schema_name:
            The schema to move the table to.

        """
        self._set_schema.append(SetSchema(schema_name=schema_name))
        return self

    @property
    def default_ddl(self) -> Sequence[str]:
        if self._drop_table is not None:
            return [self._drop_table.ddl]

        query = f"ALTER TABLE {self.table._meta.get_formatted_tablename()}"

        alterations = [
            i.ddl
            for i in itertools.chain(
                self._add,
                self._add_foreign_key_constraint,
                self._rename_columns,
                self._rename_table,
                self._rename_constraint,
                self._drop,
                self._drop_constraint,
                self._drop_default,
                self._set_column_type,
                self._set_unique,
                self._set_null,
                self._set_length,
                self._set_default,
                self._set_digits,
                self._set_schema,
            )
        ]

        if self.engine_type == "sqlite":
            # Can only perform one alter statement at a time.
            return [f"{query} {i}" for i in alterations]

        # Postgres can perform them all at once:
        query += ",".join(f" {i}" for i in alterations)

        return [query]


Self = TypeVar("Self", bound=Alter)
