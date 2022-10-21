import functools
from typing import Callable

import sqlalchemy

from procrastinate import exceptions


def wrap_exceptions(func: Callable) -> Callable:
    """
    Wrap SQLAlchemy errors as connector exceptions.
    """

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except sqlalchemy.exc.IntegrityError as exc:
            raise exceptions.ConnectorException from exc
        except sqlalchemy.exc.SQLAlchemyError as exc:
            raise exceptions.ConnectorException from exc

    # Attaching a custom attribute to ease testability and make the
    # decorator more introspectable
    wrapped._exceptions_wrapped = True  # type: ignore
    return wrapped


def wrap_query_exceptions(func: Callable) -> Callable:
    """
    Detect "admin shutdown" errors and retry once.

    This is to handle the case where the database connection (obtained from the pool)
    was actually closed by the server. In this case, SQLAlchemy raises a ``DBAPIError``
    with ``connection_invalidated`` set to ``True``, and also invalidates the rest of
    the connection pool. So we just retry once, to get a fresh connection.
    """

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except sqlalchemy.exc.DBAPIError as exc:
            if exc.connection_invalidated:
                return func(*args, **kwargs)
            raise exc

    return wrapped
