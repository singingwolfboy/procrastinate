import re
from typing import Any, Callable, Dict, List, Mapping, Optional

import sqlalchemy

from procrastinate import exceptions
from procrastinate.connector.base import BaseConnector
from procrastinate.contrib.sqlalchemy.utils import (
    wrap_exceptions,
    wrap_query_exceptions,
)

PERCENT_PATTERN = re.compile(r"%(?![\(s])")


class SQLAlchemyConnector(BaseConnector):
    def __init__(
        self,
        *,
        dsn: str = "postgresql://",
        json_dumps: Optional[Callable] = None,
        json_loads: Optional[Callable] = None,
        **kwargs: Any,
    ):
        """
        Synchronous connector based on SQLAlchemy.

        This is used if you want your ``.defer()`` calls to be purely synchronous, not
        asynchronous with a sync wrapper. You may need this if your program is
        multi-threaded and doen't handle async loops well
        (see `discussion-sync-defer`).

        All other arguments than ``dsn``, ``json_dumps``, and ``json_loads`` are passed
        to :py:func:`create_engine` (see SQLAlchemy documentation__).

        .. __: https://docs.sqlalchemy.org/en/latest/core/engines.html#sqlalchemy.create_engine

        Parameters
        ----------
        dsn : The dsn string or URL object passed to SQLAlchemy's ``create_engine``
            function. Ignored if the engine is externally created and set into the
            connector through the ``App.open`` method.
        json_dumps :
            The JSON dumps function to use for serializing job arguments. Defaults to
            the function used by psycopg2. See the `psycopg2 doc`_.
        json_loads :
            The JSON loads function to use for deserializing job arguments. Defaults
            Python's ``json.loads`` function.
        """
        self.json_dumps = json_dumps
        self.json_loads = json_loads
        self._engine: Optional[sqlalchemy.engine.Engine] = None
        self._engine_dsn = dsn
        self._engine_kwargs = kwargs
        self._engine_externally_set = False

    @wrap_exceptions
    def open(self, engine: Optional[sqlalchemy.engine.Engine] = None) -> None:
        """
        Create an SQLAlchemy engine for the connector.

        Parameters
        ----------
        engine :
            Optional engine. Procrastinate can use an existing engine. If set the
            engine dsn and arguments passed in the constructor will be ignored.
        """
        if engine:
            self._engine_externally_set = True
            self._engine = engine
        else:
            self._engine = self._create_engine(self._engine_dsn, self._engine_kwargs)

    @staticmethod
    def _create_engine(
        dsn: str, engine_kwargs: Dict[str, Any]
    ) -> sqlalchemy.engine.Engine:
        """
        Create an SQLAlchemy engine.
        """
        return sqlalchemy.create_engine(url=dsn, **engine_kwargs)

    @wrap_exceptions
    def close(self) -> None:
        """
        Dispose of the connection pool used by the SQLAlchemy engine.
        """
        if not self._engine_externally_set and self._engine:
            self._engine.dispose()
        self._engine = None

    @property
    def engine(self) -> sqlalchemy.engine.Engine:
        if self._engine is None:  # Set by open
            raise exceptions.AppNotOpen
        return self._engine

    def _wrap_json(self, arguments: Dict[str, Any]):
        return arguments

    @wrap_exceptions
    @wrap_query_exceptions
    def execute_query(self, query: str, **arguments: Any) -> None:
        with self.engine.begin() as connection:
            connection.exec_driver_sql(
                PERCENT_PATTERN.sub("%%", query), self._wrap_json(arguments)
            )

    @wrap_exceptions
    @wrap_query_exceptions
    def execute_query_one(self, query: str, **arguments: Any) -> Mapping[str, Any]:
        with self.engine.begin() as connection:
            cursor_result = connection.exec_driver_sql(
                PERCENT_PATTERN.sub("%%", query), self._wrap_json(arguments)
            )
            mapping = cursor_result.mappings()
            return mapping.fetchone()

    @wrap_exceptions
    @wrap_query_exceptions
    def execute_query_all(
        self, query: str, **arguments: Any
    ) -> List[Mapping[str, Any]]:
        with self.engine.begin() as connection:
            cursor_result = connection.exec_driver_sql(
                PERCENT_PATTERN.sub("%%", query), self._wrap_json(arguments)
            )
            mapping = cursor_result.mappings()
            return mapping.all()
