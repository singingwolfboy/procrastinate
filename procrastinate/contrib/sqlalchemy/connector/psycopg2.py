import functools
import re
from typing import Any, Callable, Dict, List, Mapping, Optional

import psycopg2.errors
import sqlalchemy
from psycopg2.extras import Json

from procrastinate import exceptions
from procrastinate.contrib.sqlalchemy import SQLAlchemyConnector, wrap_query_exceptions


def wrap_exceptions(func: Callable) -> Callable:
    """
    Wrap SQLAlchemy errors as connector exceptions.
    """

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except sqlalchemy.exc.IntegrityError as exc:
            if isinstance(exc.orig, psycopg2.errors.UniqueViolation):
                raise exceptions.UniqueViolation(
                    constraint_name=exc.orig.diag.constraint_name
                )
            raise exceptions.ConnectorException from exc
        except sqlalchemy.exc.SQLAlchemyError as exc:
            raise exceptions.ConnectorException from exc

    # Attaching a custom attribute to ease testability and make the
    # decorator more introspectable
    wrapped._exceptions_wrapped = True  # type: ignore
    return wrapped


PERCENT_PATTERN = re.compile(r"%(?![\(s])")


class SQLAlchemyPsycopg2Connector(SQLAlchemyConnector):
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

    @wrap_exceptions
    def close(self) -> None:
        """
        Dispose of the connection pool used by the SQLAlchemy engine.
        """
        if not self._engine_externally_set and self._engine:
            self._engine.dispose()
        self._engine = None

    def _wrap_json(self, arguments: Dict[str, Any]):
        return {
            key: Json(value, dumps=self.json_dumps)
            if isinstance(value, dict)
            else value
            for key, value in arguments.items()
        }

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
