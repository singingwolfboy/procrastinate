import contextlib
import pathlib

import pytest
from django.db import connection
from migra import Migration
from sqlalchemy.pool import NullPool
from sqlbag import S

from procrastinate import schema
from procrastinate.connector.aiopg import AiopgConnector


@pytest.fixture
def run_migrations(db_execute):
    def _(dbname):
        folder = pathlib.Path(__file__).parents[2] / "procrastinate/sql/migrations"
        migrations = sorted(folder.glob("*.sql"))

        for migration in migrations:
            with db_execute(dbname) as execute:
                execute(migration.read_text())

    return _


@pytest.fixture
def schema_database(db_factory):
    dbname = "procrastinate_schema"
    db_factory(dbname=dbname)

    # apply the current procrastinate schema to the "procrastinate_schema" database
    connector = AiopgConnector(dbname=dbname)
    connector.open()
    schema_manager = schema.SchemaManager(connector=connector)
    schema_manager.apply_schema()
    connector.close()

    return dbname


@pytest.fixture
def migrations_database(db_factory, db_execute):
    dbname = "procrastinate_migrations"
    db_factory(dbname=dbname)

    return dbname


@pytest.fixture
def django_db(db):
    yield db


def test_migration(schema_database, migrations_database, run_migrations):
    # apply the migrations on the migrations_database database
    run_migrations(migrations_database)

    # use migra to verify that the databases "schema_database" and "migrations_database"
    # have nos differences

    with contextlib.ExitStack() as stack:

        # we use a NullPool to avoid issues when dropping the databases because
        # of opened database sessions
        schema_db_session = stack.enter_context(
            S(f"postgresql:///{schema_database}", poolclass=NullPool)
        )
        migrations_db_session = stack.enter_context(
            S(f"postgresql:///{migrations_database}", poolclass=NullPool)
        )
        m = Migration(schema_db_session, migrations_db_session)
        m.set_safety(False)
        m.add_all_changes()

    print(m.sql)
    assert not m.statements


def test_django_migrations_run_properly(django_db):
    # At this point, with the db fixture, we have all migrations applied
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM procrastinate_jobs")
