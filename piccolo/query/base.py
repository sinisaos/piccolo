from __future__ import annotations

from collections.abc import Generator, Sequence
from time import time
from typing import TYPE_CHECKING, Any, Generic, Optional, Union, cast

from piccolo.columns.column_types import JSON, JSONB
from piccolo.custom_types import QueryResponseType, TableInstance
from piccolo.query.mixins import ColumnsDelegate
from piccolo.query.operators.json import JSONQueryString
from piccolo.querystring import QueryString
from piccolo.utils.encoding import load_json
from piccolo.utils.objects import make_nested_object
from piccolo.utils.sync import run_sync

if TYPE_CHECKING:  # pragma: no cover
    from piccolo.query.mixins import OutputDelegate
    from piccolo.table import Table  # noqa


class Timer:
    def __enter__(self):
        self.start = time()

    def __exit__(self, exception_type, exception, traceback):
        self.end = time()
        print(f"Duration: {self.end - self.start}s")


class Query(Generic[TableInstance, QueryResponseType]):
    __slots__ = ("table", "_frozen_querystrings")

    def __init__(
        self,
        table: type[TableInstance],
        frozen_querystrings: Optional[Sequence[QueryString]] = None,
    ):
        self.table = table
        self._frozen_querystrings = frozen_querystrings

    @property
    def engine_type(self) -> str:
        engine = self.table._meta.db
        if engine:
            return engine.engine_type
        else:
            raise ValueError("Engine isn't defined.")

    async def _process_results(self, results) -> QueryResponseType:
        raw = (
            self.table._meta.db.transform_response_to_dicts(results)
            if results
            else []
        )

        if hasattr(self, "_raw_response_callback"):
            self._raw_response_callback(raw)

        output: Optional[OutputDelegate] = getattr(
            self, "output_delegate", None
        )

        #######################################################################

        if output and output._output.load_json:
            columns_delegate: Optional[ColumnsDelegate] = getattr(
                self, "columns_delegate", None
            )

            json_column_names: list[str] = []

            if columns_delegate is not None:
                json_columns: list[Union[JSON, JSONB]] = []

                for column in columns_delegate.selected_columns:
                    if isinstance(column, (JSON, JSONB)):
                        json_columns.append(column)
                    elif isinstance(column, JSONQueryString):
                        if alias := column._alias:
                            json_column_names.append(alias)
            else:
                json_columns = self.table._meta.json_columns

            for column in json_columns:
                if column._alias is not None:
                    json_column_names.append(column._alias)
                elif len(column._meta.call_chain) > 0:
                    json_column_names.append(column._meta.get_default_alias())
                else:
                    json_column_names.append(column._meta.name)

            processed_raw = []

            for row in raw:
                new_row = {**row}
                for json_column_name in json_column_names:
                    value = new_row.get(json_column_name)
                    if value is not None:
                        new_row[json_column_name] = load_json(value)
                processed_raw.append(new_row)

            raw = processed_raw

        #######################################################################

        raw = await self.response_handler(raw)

        if output:
            if output._output.as_objects:
                if output._output.nested:
                    return cast(
                        QueryResponseType,
                        [make_nested_object(row, self.table) for row in raw],
                    )
                else:
                    return cast(
                        QueryResponseType,
                        [
                            self.table(**columns, _exists_in_db=True)
                            for columns in raw
                        ],
                    )

        return cast(QueryResponseType, raw)

    def _validate(self):
        """
        Override in any subclasses if validation needs to be run before
        executing a query - for example, warning a user if they're about to
        delete all the data from a table.
        """
        pass

    def __await__(self) -> Generator[None, None, QueryResponseType]:
        """
        If the user doesn't explicity call .run(), proxy to it as a
        convenience.
        """
        return self.run().__await__()

    async def _run(
        self, node: Optional[str] = None, in_pool: bool = True
    ) -> QueryResponseType:
        """
        Run the query on the database.

        :param node:
            If specified, run this query against another database node. Only
            available in Postgres. See :class:`PostgresEngine <piccolo.engine.postgres.PostgresEngine>`.
        :param in_pool:
            Whether to run this in a connection pool if one is available. This
            is mostly just for debugging - use a connection pool where
            possible.

        """  # noqa: E501
        self._validate()

        engine = self.table._meta.db

        if not engine:
            raise ValueError(
                f"Table {self.table._meta.tablename} has no db defined in "
                "_meta"
            )

        if node is not None:
            from piccolo.engine.postgres import PostgresEngine

            if isinstance(engine, PostgresEngine):
                engine = engine.extra_nodes[node]

        querystrings = self.querystrings

        if len(querystrings) == 1:
            results = await engine.run_querystring(
                querystrings[0], in_pool=in_pool
            )
            return await self._process_results(results)
        else:
            responses = []
            for querystring in querystrings:
                results = await engine.run_querystring(
                    querystring, in_pool=in_pool
                )
                processed_results = await self._process_results(results)

                responses.append(processed_results)
            return cast(QueryResponseType, responses)

    async def run(
        self, node: Optional[str] = None, in_pool: bool = True
    ) -> QueryResponseType:
        return await self._run(node=node, in_pool=in_pool)

    def run_sync(
        self,
        node: Optional[str] = None,
        timed: bool = False,
        in_pool: bool = False,
    ) -> QueryResponseType:
        """
        A convenience method for running the coroutine synchronously.

        :param timed:
            If ``True``, the time taken to run the query is printed out. Useful
            for debugging.
        :param in_pool:
            Whether to run this in a connection pool if one is available. Set
            to ``False`` by default, because if an app uses ``run`` and
            ``run_sync`` in the same app, it can cause errors. See
            `issue 505 <https://github.com/piccolo-orm/piccolo/issues/505>`_.

        """
        coroutine = self.run(node=node, in_pool=in_pool)

        if not timed:
            return run_sync(coroutine)
        with Timer():
            return run_sync(coroutine)

    async def response_handler(self, response: list) -> Any:
        """
        Subclasses can override this to modify the raw response returned by
        the database driver.
        """
        return response

    ###########################################################################

    @property
    def sqlite_querystrings(self) -> Sequence[QueryString]:
        raise NotImplementedError

    @property
    def postgres_querystrings(self) -> Sequence[QueryString]:
        raise NotImplementedError

    @property
    def cockroach_querystrings(self) -> Sequence[QueryString]:
        raise NotImplementedError

    @property
    def default_querystrings(self) -> Sequence[QueryString]:
        raise NotImplementedError

    @property
    def querystrings(self) -> Sequence[QueryString]:
        """
        Calls the correct underlying method, depending on the current engine.
        """
        if self._frozen_querystrings is not None:
            return self._frozen_querystrings

        engine_type = self.engine_type
        if engine_type == "postgres":
            try:
                return self.postgres_querystrings
            except NotImplementedError:
                return self.default_querystrings
        elif engine_type == "sqlite":
            try:
                return self.sqlite_querystrings
            except NotImplementedError:
                return self.default_querystrings
        elif engine_type == "cockroach":
            try:
                return self.cockroach_querystrings
            except NotImplementedError:
                return self.default_querystrings
        else:
            raise Exception(
                f"No querystring found for the {engine_type} engine."
            )

    ###########################################################################

    def freeze(self) -> FrozenQuery:
        """
        This is a performance optimisation when the same query is run
        repeatedly. For example:

        .. code-block:: python

            TOP_BANDS = Band.select(
                Band.name
            ).order_by(
                Band.popularity,
                ascending=False
            ).limit(
                10
            ).output(
                as_json=True
            ).freeze()

            # In the corresponding view/endpoint of whichever web framework
            # you're using:
            async def top_bands(self, request):
                return await TOP_BANDS

        It means that Piccolo doesn't have to work as hard each time the query
        is run to generate the corresponding SQL - some of it is cached. If the
        query is defined within the view/endpoint, it has to generate the SQL
        from scratch each time.

        Once a query is frozen, you can't apply any more clauses to it
        (``where``, ``limit``, ``output`` etc).

        Even though ``freeze`` helps with performance, there are limits to
        how much it can help, as most of the time is still spent waiting for a
        response from the database. However, for high throughput apps and data
        science scripts, it's a worthwhile optimisation.

        """
        querystrings = self.querystrings
        for querystring in querystrings:
            querystring.freeze(engine_type=self.engine_type)

        # Copy the query, so we don't store any references to the original.
        query = self.__class__(
            table=self.table, frozen_querystrings=querystrings
        )

        if hasattr(self, "limit_delegate"):
            # Needed for `response_handler`
            query.limit_delegate = self.limit_delegate.copy()  # type: ignore

        if hasattr(self, "output_delegate"):
            # Needed for `_process_results`
            query.output_delegate = self.output_delegate.copy()  # type: ignore

        return FrozenQuery(query=query)

    ###########################################################################

    def __str__(self) -> str:
        return "; ".join([i.__str__() for i in self.querystrings])


###############################################################################


class FrozenQuery:
    def __init__(self, query: Query):
        self.query = query

    async def run(self, *args, **kwargs):
        return await self.query.run(*args, **kwargs)

    def run_sync(self, *args, **kwargs):
        return self.query.run_sync(*args, **kwargs)

    def __getattr__(self, name: str):
        if hasattr(self.query, name):
            raise AttributeError(
                f"This query is frozen - {name} is only available on "
                "unfrozen queries."
            )
        else:
            raise AttributeError("Unrecognised attribute name.")

    def __str__(self) -> str:
        return self.query.__str__()


###############################################################################


class DDL:
    __slots__ = ("table",)

    def __init__(self, table: type[Table], **kwargs):
        self.table = table

    @property
    def engine_type(self) -> str:
        engine = self.table._meta.db
        if engine:
            return engine.engine_type
        else:
            raise ValueError("Engine isn't defined.")

    @property
    def sqlite_ddl(self) -> Sequence[str]:
        raise NotImplementedError

    @property
    def postgres_ddl(self) -> Sequence[str]:
        raise NotImplementedError

    @property
    def cockroach_ddl(self) -> Sequence[str]:
        raise NotImplementedError

    @property
    def default_ddl(self) -> Sequence[str]:
        raise NotImplementedError

    @property
    def ddl(self) -> Sequence[str]:
        """
        Calls the correct underlying method, depending on the current engine.
        """
        engine_type = self.engine_type
        if engine_type == "postgres":
            try:
                return self.postgres_ddl
            except NotImplementedError:
                return self.default_ddl
        elif engine_type == "sqlite":
            try:
                return self.sqlite_ddl
            except NotImplementedError:
                return self.default_ddl
        elif engine_type == "cockroach":
            try:
                return self.cockroach_ddl
            except NotImplementedError:
                return self.default_ddl
        else:
            raise Exception(
                f"No querystring found for the {engine_type} engine."
            )

    def __await__(self):
        """
        If the user doesn't explicity call .run(), proxy to it as a
        convenience.
        """
        return self.run().__await__()

    async def run(self, in_pool=True):
        engine = self.table._meta.db
        if not engine:
            raise ValueError(
                f"Table {self.table._meta.tablename} has no db defined in "
                "_meta"
            )

        if len(self.ddl) == 1:
            return await engine.run_ddl(self.ddl[0], in_pool=in_pool)
        responses = []
        for ddl in self.ddl:
            response = await engine.run_ddl(ddl, in_pool=in_pool)
            responses.append(response)
        return responses

    def run_sync(self, timed=False, *args, **kwargs):
        """
        A convenience method for running the coroutine synchronously.
        """
        coroutine = self.run(*args, **kwargs, in_pool=False)

        if not timed:
            return run_sync(coroutine)
        with Timer():
            return run_sync(coroutine)

    def __str__(self) -> str:
        return self.ddl.__str__()
