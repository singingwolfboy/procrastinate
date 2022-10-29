import asyncio
import functools
import json

import attr
import pytest

from procrastinate.connector.asyncpg import AsyncpgConnector


@pytest.fixture
async def asyncpg_connector_factory(connection_params):
    connectors = []

    async def _(**kwargs):
        json_dumps = kwargs.pop("json_dumps", None)
        json_loads = kwargs.pop("json_loads", None)
        connection_params.update(kwargs)
        connector = AsyncpgConnector(
            json_dumps=json_dumps, json_loads=json_loads, **connection_params
        )
        connectors.append(connector)
        await connector.open_async()
        return connector

    yield _
    for connector in connectors:
        await connector.close_async()


@pytest.mark.parametrize(
    "method_name, expected",
    [
        ("execute_query_one_async", {"json": {"a": "a", "b": "foo"}}),
        ("execute_query_all_async", [{"json": {"a": "a", "b": "foo"}}]),
    ],
)
async def test_execute_query_json_dumps(
    asyncpg_connector_factory, mocker, method_name, expected
):
    class NotJSONSerializableByDefault:
        pass

    def encode(obj):
        if isinstance(obj, NotJSONSerializableByDefault):
            return "foo"
        raise TypeError()

    query = "SELECT %(arg)s::jsonb as json"
    arg = {"a": "a", "b": NotJSONSerializableByDefault()}
    json_dumps = functools.partial(json.dumps, default=encode)
    connector = await asyncpg_connector_factory(json_dumps=json_dumps)
    method = getattr(connector, method_name)

    result = await method(query, arg=arg)
    assert result == expected


async def test_json_loads(asyncpg_connector_factory, mocker):
    @attr.dataclass
    class Param:
        p: int

    def decode(dct):
        if "b" in dct:
            dct["b"] = Param(p=dct["b"])
        return dct

    json_loads = functools.partial(json.loads, object_hook=decode)

    query = "SELECT %(arg)s::jsonb as json"
    arg = {"a": 1, "b": 2}
    connector = await asyncpg_connector_factory(json_loads=json_loads)

    result = await connector.execute_query_one_async(query, arg=arg)
    assert result["json"] == {"a": 1, "b": Param(p=2)}


async def test_execute_query(asyncpg_connector):
    assert (
        await asyncpg_connector.execute_query_async(
            "COMMENT ON TABLE \"procrastinate_jobs\" IS 'foo' "
        )
        is None
    )
    result = await asyncpg_connector.execute_query_one_async(
        "SELECT obj_description('public.procrastinate_jobs'::regclass)"
    )
    assert result == {"obj_description": "foo"}

    result = await asyncpg_connector.execute_query_all_async(
        "SELECT obj_description('public.procrastinate_jobs'::regclass)"
    )
    assert result == [{"obj_description": "foo"}]


async def test_execute_query_no_interpolate(asyncpg_connector):
    result = await asyncpg_connector.execute_query_one_async("SELECT '%(foo)s' as foo;")
    assert result == {"foo": "%(foo)s"}


async def test_execute_query_interpolate(asyncpg_connector):
    result = await asyncpg_connector.execute_query_one_async(
        "SELECT %(foo)s as foo;", foo="bar"
    )
    assert result == {"foo": "bar"}


@pytest.mark.filterwarnings("error::ResourceWarning")
async def test_execute_query_simultaneous(asyncpg_connector):
    # two coroutines doing execute_query_async simulteneously
    #
    # the test may fail if the connector fails to properly parallelize connections

    async def query():
        await asyncpg_connector.execute_query_async("SELECT 1")

    try:
        await asyncio.gather(query(), query())
    except ResourceWarning:
        pytest.fail("ResourceWarning")


async def test_close_async(asyncpg_connector):
    await asyncpg_connector.execute_query_async("SELECT 1")
    pool = asyncpg_connector._pool
    await asyncpg_connector.close_async()
    assert pool._closed is True
    assert asyncpg_connector._pool is None


async def test_get_connection_no_psycopg2_adapter_registration(
    asyncpg_connector_factory, mocker
):
    register_adapter = mocker.patch("psycopg2.extensions.register_adapter")
    connector = await asyncpg_connector_factory()
    await connector.open_async()
    assert not register_adapter.called


async def test_listen_notify(asyncpg_connector):
    channel = "somechannel"
    event = asyncio.Event()

    task = asyncio.ensure_future(
        asyncpg_connector.listen_notify(channels=[channel], event=event)
    )
    try:
        await event.wait()
        event.clear()
        await asyncpg_connector.execute_query_async(f"""NOTIFY "{channel}" """)
        await asyncio.wait_for(event.wait(), timeout=1)
    except asyncio.TimeoutError:
        pytest.fail("Notify not received within 1 sec")
    finally:
        task.cancel()
