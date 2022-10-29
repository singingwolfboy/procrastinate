import contextlib
import functools
import itertools
import os
import random
import signal as stdlib_signal
import string
import uuid

import aiopg
import psycopg2
import pytest
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from procrastinate import app as app_module
from procrastinate import blueprints, builtin_tasks, jobs, schema, testing
from procrastinate.connector.aiopg import AiopgConnector
from procrastinate.connector.asyncpg import AsyncpgConnector
from procrastinate.connector.psycopg2 import Psycopg2Connector
from procrastinate.contrib.sqlalchemy.connector.psycopg2 import (
    SQLAlchemyPsycopg2Connector,
)

# Just ensuring the tests are not polluted by environment
for key in os.environ:
    if key.startswith("PROCRASTINATE_"):
        os.environ.pop(key)


def cursor_execute(cursor, query, *identifiers, format=True):
    if identifiers:
        query = sql.SQL(query).format(
            *(sql.Identifier(identifier) for identifier in identifiers)
        )
    cursor.execute(query)


@contextlib.contextmanager
def db_executor(dbname):
    with contextlib.closing(psycopg2.connect("", dbname=dbname)) as connection:
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with connection.cursor() as cursor:
            yield functools.partial(cursor_execute, cursor)


@pytest.fixture
def db_execute():
    return db_executor


def db_create(dbname, template=None):
    with db_executor("postgres") as execute:
        execute("DROP DATABASE IF EXISTS {}", dbname)
        if template:
            execute("CREATE DATABASE {} TEMPLATE {}", dbname, template)
        else:
            execute("CREATE DATABASE {}", dbname)


def db_drop(dbname):
    with db_executor("postgres") as execute:
        execute("DROP DATABASE IF EXISTS {}", dbname)


@pytest.fixture
def db_factory():
    dbs_to_drop = []

    def _(dbname, template=None):
        db_create(dbname=dbname, template=template)
        dbs_to_drop.append(dbname)

    yield _

    for dbname in dbs_to_drop:
        db_drop(dbname=dbname)


@pytest.fixture(scope="session")
def setup_db():

    dbname = "procrastinate_test_template"
    db_create(dbname=dbname)

    connector = AiopgConnector(dbname=dbname)
    connector.open()
    schema_manager = schema.SchemaManager(connector=connector)
    schema_manager.apply_schema()
    # We need to close the psycopg2 underlying connection synchronously
    connector.close()

    yield dbname

    db_drop(dbname=dbname)


@pytest.fixture
def connection_params(setup_db, db_factory):
    db_factory(dbname="procrastinate_test", template=setup_db)

    yield {"dsn": "", "dbname": "procrastinate_test"}


@pytest.fixture
def sqlalchemy_engine_dsn(setup_db, db_factory):
    db_factory(dbname="procrastinate_test", template=setup_db)

    yield "postgresql+psycopg2:///procrastinate_test"


@pytest.fixture
async def connection(connection_params):  # TODO: rename to `aiopg_connection` ?
    async with aiopg.connect(**connection_params) as connection:
        yield connection


@pytest.fixture
async def not_opened_aiopg_connector(connection_params):
    yield AiopgConnector(**connection_params)


@pytest.fixture
def not_opened_asyncpg_connector(connection_params):
    yield AsyncpgConnector(**connection_params)


@pytest.fixture
def not_opened_psycopg2_connector(connection_params):
    yield Psycopg2Connector(**connection_params)


@pytest.fixture
def not_opened_sqlalchemy_psycopg2_connector(sqlalchemy_engine_dsn):
    yield SQLAlchemyPsycopg2Connector(dsn=sqlalchemy_engine_dsn, echo=True)


@pytest.fixture
async def aiopg_connector(not_opened_aiopg_connector):
    await not_opened_aiopg_connector.open_async()
    yield not_opened_aiopg_connector
    await not_opened_aiopg_connector.close_async()


@pytest.fixture
async def asyncpg_connector(not_opened_asyncpg_connector):
    await not_opened_asyncpg_connector.open_async()
    yield not_opened_asyncpg_connector
    await not_opened_asyncpg_connector.close_async()


@pytest.fixture
def psycopg2_connector(not_opened_psycopg2_connector):
    not_opened_psycopg2_connector.open()
    yield not_opened_psycopg2_connector
    not_opened_psycopg2_connector.close()


@pytest.fixture
def sqlalchemy_psycopg2_connector(not_opened_sqlalchemy_psycopg2_connector):
    not_opened_sqlalchemy_psycopg2_connector.open()
    yield not_opened_sqlalchemy_psycopg2_connector
    not_opened_sqlalchemy_psycopg2_connector.close()


@pytest.fixture
def kill_own_pid():
    def f(signal=stdlib_signal.SIGTERM):
        os.kill(os.getpid(), signal)

    return f


@pytest.fixture
def connector():
    return testing.InMemoryConnector()


@pytest.fixture
def reset_builtin_task_names():
    builtin_tasks.remove_old_jobs.name = "procrastinate.builtin_tasks.remove_old_jobs"
    builtin_tasks.builtin.tasks = {
        task.name: task for task in builtin_tasks.builtin.tasks.values()
    }


@pytest.fixture
def not_opened_app(connector, reset_builtin_task_names):
    return app_module.App(connector=connector)


@pytest.fixture
def app(not_opened_app):
    with not_opened_app.open() as app:
        yield app


@pytest.fixture
def blueprint():
    return blueprints.Blueprint()


@pytest.fixture
def job_manager(app):
    return app.job_manager


@pytest.fixture
def serial():
    return itertools.count(1)


@pytest.fixture
def random_str():
    def _(length=8):
        return "".join(random.choice(string.ascii_lowercase) for _ in range(length))

    return _


@pytest.fixture
def job_factory(serial, random_str):
    def factory(**kwargs):
        defaults = {
            "id": next(serial),
            "task_name": f"task_{random_str()}",
            "task_kwargs": {},
            "lock": str(uuid.uuid4()),
            "queueing_lock": None,
            "queue": f"queue_{random_str()}",
        }
        final_kwargs = defaults.copy()
        final_kwargs.update(kwargs)
        return jobs.Job(**final_kwargs)

    return factory


@pytest.fixture
def deferred_job_factory(job_factory, job_manager):
    async def factory(*, job_manager=job_manager, **kwargs):
        job = job_factory(id=None, **kwargs)
        return await job_manager.defer_job_async(job)

    return factory
