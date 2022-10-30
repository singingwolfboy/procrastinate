import asyncio
import functools
import json
import logging
from typing import Any, Callable, Coroutine, Dict, Iterable, List, Optional

import asyncpg

from procrastinate import exceptions
from procrastinate.connector.base import BaseAsyncConnector

logger = logging.getLogger(__name__)

CoroutineFunction = Callable[..., Coroutine]


def wrap_exceptions(coro: CoroutineFunction) -> CoroutineFunction:
    """
    Wrap asyncpg errors as connector exceptions.

    This decorator is expected to be used on coroutine functions only.
    """

    @functools.wraps(coro)
    async def wrapped(*args, **kwargs):
        try:
            return await coro(*args, **kwargs)
        except asyncpg.UniqueViolationError as exc:
            raise exceptions.UniqueViolation(constraint_name=exc.diag.constraint_name)
        except asyncpg.PostgresError as exc:
            raise exceptions.ConnectorException from exc

    # Attaching a custom attribute to ease testability and make the
    # decorator more introspectable
    wrapped._exceptions_wrapped = True  # type: ignore
    return wrapped


def wrap_query_exceptions(coro: CoroutineFunction) -> CoroutineFunction:
    """
    Detect asyncpg InterfaceError's with a "server closed the connection unexpectedly"
    message and retry a number of times.

    This is to handle the case where the database connection (obtained from the pool)
    was actually closed by the server. In this case, asyncpg raises an InterfaceError
    with a "server closed the connection unexpectedly" message (and no pgcode) when the
    connection is used for issuing a query. What we do is retry when an InterfaceError
    is raised, and until the maximum number of retries is reached.

    The number of retries is set to the pool maximum size plus one, to handle the case
    where the connections we have in the pool were all closed on the server side.
    """

    @functools.wraps(coro)
    async def wrapped(*args, **kwargs):
        final_exc = None
        try:
            max_tries = args[0]._pool.maxsize + 1
        except Exception:
            max_tries = 1
        for _ in range(max_tries):
            try:
                return await coro(*args, **kwargs)
            except asyncpg.InterfaceError as exc:
                if "server closed the connection unexpectedly" in str(exc):
                    final_exc = exc
                    continue
                raise exc
        raise exceptions.ConnectorException(
            f"Could not get a valid connection after {max_tries} tries"
        ) from final_exc

    return wrapped


class AsyncpgQueryConverter:
    def __init__(self, arg_dict: Dict[str, Any]):
        self.order: List[str] = []
        self.arg_dict = arg_dict

    def __getitem__(self, key: str):
        if key in self.arg_dict:
            # We need to convert this. Have we done so already?
            if key in self.order:
                return f"${self.order.index(key) + 1}"
            else:
                # Make a new positional query arg
                self.order.append(key)
                return f"${len(self.order)}"
        else:
            # We don't need to convert this, so put it back
            # as a named argument
            return f"%({key})s"

    @property
    def positional_args(self):
        return tuple(self.arg_dict[arg_name] for arg_name in self.order)


class AsyncpgConnector(BaseAsyncConnector):
    def __init__(
        self,
        *,
        json_dumps: Optional[Callable] = None,
        json_loads: Optional[Callable] = None,
        **kwargs: Any,
    ):
        """
        Create a PostgreSQL connector using asyncpg. The connector uses an ``asyncpg.Pool``, either
        created internally or specified when initializing the `AsyncpgConnector` in the pool parameter.

        All other arguments than ``json_dumps`` and ``json_loads`` are passed to
        :py:func:`asyncpg.create_pool` (see asyncpg documentation__), with default values
        that may differ from those of ``asyncpg`` (see the list of parameters below).

        .. __: https://magicstack.github.io/asyncpg/current/api/index.html#asyncpg.pool.create_pool

        Parameters
        ----------
        json_dumps :
            The JSON dumps function to use for serializing job arguments.
        json_loads :
            The JSON loads function to use for deserializing job arguments.
            Unused if the pool is externally created and set into the connector
            through the ``App.open_async`` method.
        """
        self._pool: Optional[asyncpg.Pool] = None
        self._pool_externally_set: bool = False
        self.json_dumps = json_dumps
        self.json_loads = json_loads
        self._pool_args = self._adapt_pool_args(
            kwargs, json_dumps=json_dumps, json_loads=json_loads
        )
        self._lock: Optional[asyncio.Lock] = None

    @staticmethod
    def _adapt_pool_args(
        pool_args: Dict[str, Any],
        json_dumps: Optional[Callable] = None,
        json_loads: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        """
        Adapt the pool args for ``asyncpg``, using sensible defaults for Procrastinate.
        """
        if "dbname" in pool_args and "database" in pool_args:
            raise exceptions.ConnectorException(
                "Can not pass both `dbname` and `database` to `AsyncpgConnector`"
            )
        dbname = pool_args.get("dbname")
        if dbname:
            pool_args["database"] = dbname
            del pool_args["dbname"]

        extra_init = pool_args.get("init", None)

        async def init(conn):
            for json_type_name in ("json", "jsonb"):
                await conn.set_type_codec(
                    json_type_name,
                    encoder=json_dumps or json.dumps,
                    decoder=json_loads or json.loads,
                    schema="pg_catalog",
                )
            if extra_init:
                await extra_init(conn)

        pool_args["init"] = init

        return pool_args

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:  # Set by open_async
            raise exceptions.AppNotOpen
        return self._pool

    async def open_async(self, pool: Optional[asyncpg.Pool] = None) -> None:
        if self._pool:
            return
        if pool:
            self._pool_externally_set = True
            self._pool = pool
        else:
            self._pool = await self._create_pool(self._pool_args)

    @staticmethod
    @wrap_exceptions
    async def _create_pool(pool_args: Dict[str, Any]) -> asyncpg.Pool:
        pool = await asyncpg.create_pool(**pool_args)
        if not pool:
            # just to make mypy happy; see
            # https://github.com/bryanforbes/asyncpg-stubs/issues/120
            raise ValueError("could not create asyncpg pool")
        return pool

    @wrap_exceptions
    async def close_async(self) -> None:
        """
        Close the pool and awaits all connections to be released.
        """

        if not self._pool or self._pool_externally_set:
            return
        await self._pool.close()
        self._pool = None

    async def __del__(self):
        if self._pool and not self._pool_externally_set:
            # Consider https://docs.python.org/3/library/asyncio-task.html#asyncio.wait_for
            await asyncio.wait_for(self._pool.close(), 3)

    @wrap_exceptions
    @wrap_query_exceptions
    async def execute_query_async(self, query: str, **arguments: Any) -> None:
        converter = AsyncpgQueryConverter(arguments)
        converted_query = query % converter

        async with self.pool.acquire() as conn:
            args = converter.positional_args
            await conn.execute(converted_query, *args)

    @wrap_exceptions
    @wrap_query_exceptions
    async def execute_query_one_async(
        self, query: str, **arguments: Any
    ) -> Dict[str, Any]:
        converter = AsyncpgQueryConverter(arguments)
        converted_query = query % converter

        async with self.pool.acquire() as conn:
            args = converter.positional_args
            row_record = await conn.fetchrow(converted_query, *args)
            if not row_record:
                raise ValueError("No row returned for execute_query_one_async")
            return {key: value for key, value in row_record.items()}

    @wrap_exceptions
    @wrap_query_exceptions
    async def execute_query_all_async(
        self, query: str, **arguments: Any
    ) -> List[Dict[str, Any]]:
        converter = AsyncpgQueryConverter(arguments)
        converted_query = query % converter

        async with self.pool.acquire() as conn:
            args = converter.positional_args
            # consider using a cursor instead
            rows = await conn.fetch(converted_query, *args)

            return list(
                {key: value for key, value in row_record.items()} for row_record in rows
            )

    @wrap_exceptions
    async def listen_notify(
        self, event: asyncio.Event, channels: Iterable[str]
    ) -> None:
        async def listen_callback(conn, pid, channel, payload):
            event.set()

        async def setup(conn):
            for channel in channels:
                await conn.add_listener(channel, listen_callback)

        # This is an awful hack, because the `Pool` public API does not allow
        # you to add a `setup` function after the pool object has already been
        # created. So we'll just reach into the private API and make it work.
        self.pool._setup = setup  # type: ignore
        for holder in self.pool._holders:  # type: ignore
            holder._setup = setup

        # Initial set() lets caller know that we're ready to listen
        event.set()
