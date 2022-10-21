import datetime
import uuid

import pytest

from procrastinate import exceptions, jobs, manager


async def test_manager_defer_job(job_manager, job_factory, connector):
    job = await job_manager.defer_job_async(
        job=job_factory(
            task_kwargs={"a": "b"}, queue="marsupilami", task_name="bla", lock="sher"
        )
    )

    assert job.id == 1

    assert connector.jobs == {
        1: {
            "args": {"a": "b"},
            "attempts": 0,
            "id": 1,
            "lock": "sher",
            "queueing_lock": None,
            "queue_name": "marsupilami",
            "scheduled_at": None,
            "status": "todo",
            "task_name": "bla",
        }
    }


async def test_manager_defer_job_no_lock(job_manager, job_factory, connector):
    await job_manager.defer_job_async(job=job_factory())

    assert uuid.UUID(connector.jobs[1]["lock"])


async def test_manager_defer_job_connector_exception(
    mocker, job_manager, job_factory, connector
):
    connector.execute_query_one_async = mocker.Mock(
        side_effect=exceptions.ConnectorException
    )

    with pytest.raises(exceptions.ConnectorException):
        await job_manager.defer_job_async(job=job_factory(task_kwargs={"a": "b"}))


async def test_manager_defer_job_unique_violation_exception(
    mocker, job_manager, job_factory, connector
):
    connector.execute_query_one_async = mocker.Mock(
        side_effect=exceptions.UniqueViolation(
            constraint_name="procrastinate_jobs_queueing_lock_idx"
        )
    )

    with pytest.raises(exceptions.AlreadyEnqueued):
        await job_manager.defer_job_async(job=job_factory(task_kwargs={"a": "b"}))


async def test_manager_defer_job_unique_violation_exception_other_constraint(
    mocker, job_manager, job_factory, connector
):
    connector.execute_query_one_async = mocker.Mock(
        side_effect=exceptions.UniqueViolation(constraint_name="some_other_constraint")
    )

    with pytest.raises(exceptions.ConnectorException):
        await job_manager.defer_job_async(job=job_factory(task_kwargs={"a": "b"}))


async def test_fetch_job_no_suitable_job(job_manager):
    assert await job_manager.fetch_job(queues=None) is None


async def test_fetch_job(job_manager, job_factory):
    job = job_factory(id=None)
    await job_manager.defer_job_async(job=job)
    expected_job = job.evolve(id=1, status="doing")
    assert await job_manager.fetch_job(queues=None) == expected_job


async def test_get_stalled_jobs_not_stalled(job_manager, job_factory):
    job = job_factory(id=1)
    await job_manager.defer_job_async(job=job)
    assert await job_manager.get_stalled_jobs(nb_seconds=1000) == []


async def test_get_stalled_jobs_stalled(job_manager, job_factory, connector):
    job = job_factory()
    await job_manager.defer_job_async(job=job)
    await job_manager.fetch_job(queues=None)
    connector.events[1][-1]["at"] = datetime.datetime(
        2000, 1, 1, tzinfo=datetime.timezone.utc
    )
    expected_job = job.evolve(id=1, status="doing")
    assert await job_manager.get_stalled_jobs(nb_seconds=1000) == [expected_job]


@pytest.mark.parametrize(
    "include_error, statuses",
    [(False, ("succeeded",)), (True, ("succeeded", "failed"))],
)
async def test_delete_old_jobs(
    job_manager, job_factory, connector, include_error, statuses, mocker
):

    await job_manager.delete_old_jobs(
        nb_hours=5, queue="marsupilami", include_error=include_error
    )
    assert connector.queries == [
        (
            "delete_old_jobs",
            {"nb_hours": 5, "queue": "marsupilami", "statuses": statuses},
        )
    ]


async def test_finish_job(job_manager, job_factory, connector):
    job = job_factory(id=1)
    await job_manager.defer_job_async(job=job)

    await job_manager.finish_job(
        job=job, status=jobs.Status.SUCCEEDED, delete_job=False
    )
    assert connector.queries[-1] == (
        "finish_job",
        {"job_id": 1, "status": "succeeded", "delete_job": False},
    )


async def test_finish_job_with_deletion(job_manager, job_factory, connector):
    job = job_factory(id=1)
    await job_manager.defer_job_async(job=job)

    await job_manager.finish_job(job=job, status=jobs.Status.SUCCEEDED, delete_job=True)
    assert connector.queries[-1] == (
        "finish_job",
        {"job_id": 1, "status": "succeeded", "delete_job": True},
    )
    assert 1 not in connector.jobs


async def test_retry_job(job_manager, job_factory, connector):
    job = job_factory(id=1)
    await job_manager.defer_job_async(job=job)
    retry_at = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)

    await job_manager.retry_job(job=job, retry_at=retry_at)
    assert connector.queries[-1] == (
        "retry_job",
        {"job_id": 1, "retry_at": retry_at},
    )


@pytest.mark.parametrize(
    "queues, channels",
    [
        (None, ["procrastinate_any_queue"]),
        (["a", "b"], ["procrastinate_queue#a", "procrastinate_queue#b"]),
    ],
)
async def test_listen_for_jobs(job_manager, connector, mocker, queues, channels):
    event = mocker.Mock()

    await job_manager.listen_for_jobs(queues=queues, event=event)
    assert connector.notify_event is event
    assert connector.notify_channels == channels


@pytest.fixture
def configure(app):
    @app.task
    def foo(timestamp):
        pass

    return foo.configure


async def test_defer_periodic_job(configure):
    deferrer = configure(task_kwargs={"timestamp": 1234567890})

    result = await deferrer.job_manager.defer_periodic_job(
        job=deferrer.job,
        periodic_id="",
        defer_timestamp=1234567890,
    )
    assert result == 1


async def test_defer_periodic_job_with_suffixes(configure):
    deferrer = configure(task_kwargs={"timestamp": 1234567890})

    result = [
        await deferrer.job_manager.defer_periodic_job(
            job=deferrer.job,
            periodic_id="1",
            defer_timestamp=1234567890,
        ),
        await deferrer.job_manager.defer_periodic_job(
            job=deferrer.job,
            periodic_id="2",
            defer_timestamp=1234567890,
        ),
    ]

    assert result == [1, 2]


async def test_defer_periodic_job_unique_violation(configure):
    deferrer1 = configure(
        queueing_lock="bla",
        task_kwargs={"timestamp": 1234567890},
    )
    deferrer2 = configure(
        queueing_lock="bla",
        task_kwargs={"timestamp": 1234567891},
    )

    await deferrer1.job_manager.defer_periodic_job(
        job=deferrer1.job,
        periodic_id="",
        defer_timestamp=1234567890,
    )
    with pytest.raises(exceptions.AlreadyEnqueued):
        await deferrer2.job_manager.defer_periodic_job(
            job=deferrer2.job,
            periodic_id="",
            defer_timestamp=1234567891,
        )


async def test_defer_periodic_job_wrong_timestamp(configure):
    deferrer = configure(
        queueing_lock="bla",
        task_kwargs={"timestamp": 1000000000},
    )

    with pytest.raises(exceptions.InvalidTimestamp):
        await deferrer.job_manager.defer_periodic_job(
            job=deferrer.job,
            periodic_id="",
            defer_timestamp=1234567890,
        )


def test_raise_already_enqueued_right_constraint(job_manager):
    class UniqueViolation(Exception):
        constraint_name = manager.QUEUEING_LOCK_CONSTRAINT

    with pytest.raises(exceptions.AlreadyEnqueued) as exc_info:
        job_manager._raise_already_enqueued(exc=UniqueViolation(), queueing_lock="foo")

    assert "queueing lock foo" in str(exc_info.value)


def test_raise_already_enqueued_wrong_constraint(job_manager):
    class UniqueViolation(Exception):
        constraint_name = "foo"

    with pytest.raises(UniqueViolation):
        job_manager._raise_already_enqueued(exc=UniqueViolation(), queueing_lock="foo")
