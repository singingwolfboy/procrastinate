import asyncio
import datetime

import pytest

from procrastinate import exceptions, utils


def test_reset(connector):
    connector.jobs = {1: {}}
    connector.reset()
    assert connector.jobs == {}


def test_generic_execute(connector):
    result = {}
    connector.reverse_queries = {"a": "b"}

    def b(**kwargs):
        result.update(kwargs)

    connector.b_youpi = b

    connector.generic_execute("a", "youpi", i="j")

    assert result == {"i": "j"}


async def test_execute_query(connector, mocker):
    connector.generic_execute = mocker.Mock()
    await connector.execute_query_async("a", b="c")
    connector.generic_execute.assert_called_with("a", "run", b="c")


async def test_execute_query_one(connector, mocker):
    connector.generic_execute = mocker.Mock()
    assert (
        await connector.execute_query_one_async("a", b="c")
        == connector.generic_execute.return_value
    )
    connector.generic_execute.assert_called_with("a", "one", b="c")


async def test_execute_query_all_async(connector, mocker):
    connector.generic_execute = mocker.Mock()
    assert (
        await connector.execute_query_all_async("a", b="c")
        == connector.generic_execute.return_value
    )
    connector.generic_execute.assert_called_with("a", "all", b="c")


def test_make_dynamic_query(connector):
    assert connector.make_dynamic_query("foo {bar}", bar="baz") == "foo baz"


def test_defer_job_one(connector):
    job = connector.defer_job_one(
        task_name="mytask",
        lock="sher",
        queueing_lock="houba",
        args={"a": "b"},
        scheduled_at=None,
        queue="marsupilami",
    )

    assert connector.jobs == {
        1: {
            "id": 1,
            "queue_name": "marsupilami",
            "task_name": "mytask",
            "lock": "sher",
            "queueing_lock": "houba",
            "args": {"a": "b"},
            "status": "todo",
            "scheduled_at": None,
            "attempts": 0,
        }
    }
    assert connector.jobs[1] == job


def test_defer_job_one_multiple_times(connector):
    connector.defer_job_one(
        task_name="mytask",
        lock=None,
        queueing_lock=None,
        args={},
        scheduled_at=None,
        queue="default",
    )
    connector.defer_job_one(
        task_name="mytask",
        lock=None,
        queueing_lock=None,
        args={},
        scheduled_at=None,
        queue="default",
    )
    assert len(connector.jobs) == 2


def test_defer_same_job_with_queueing_lock_second_time_after_first_one_succeeded(
    connector,
):
    job_data = {
        "task_name": "mytask",
        "lock": None,
        "queueing_lock": "some-lock",
        "args": {},
        "scheduled_at": None,
        "queue": "default",
    }

    # 1. Defer job with queueing-lock
    job_row = connector.defer_job_one(**job_data)
    assert len(connector.jobs) == 1

    # 2. Defering a second time should fail, as first one
    #    still in state `todo`
    with pytest.raises(exceptions.UniqueViolation):
        connector.defer_job_one(**job_data)
    assert len(connector.jobs) == 1

    # 3. Finish first job
    connector.finish_job_run(job_id=job_row["id"], status="finished", delete_job=False)

    # 4. Defering a second time should work now,
    #    as first job in state `finished`
    connector.defer_job_one(**job_data)
    assert len(connector.jobs) == 2


def test_current_locks(connector):
    connector.jobs = {
        1: {"status": "todo", "lock": "foo"},
        2: {"status": "doing", "lock": "yay"},
    }
    assert connector.current_locks == {"yay"}


def test_finished_jobs(connector):
    connector.jobs = {
        1: {"status": "todo"},
        2: {"status": "doing"},
        3: {"status": "succeeded"},
        4: {"status": "failed"},
    }
    assert connector.finished_jobs == [{"status": "succeeded"}, {"status": "failed"}]


def test_select_stalled_jobs_all(connector):
    connector.jobs = {
        # We're not selecting this job because it's "succeeded"
        1: {
            "id": 1,
            "status": "succeeded",
            "queue_name": "marsupilami",
            "task_name": "mytask",
        },
        # This one because it's the wrong queue
        2: {
            "id": 2,
            "status": "doing",
            "queue_name": "other_queue",
            "task_name": "mytask",
        },
        # This one because of the task
        3: {
            "id": 3,
            "status": "doing",
            "queue_name": "marsupilami",
            "task_name": "my_other_task",
        },
        # This one because it's not stalled
        4: {
            "id": 4,
            "status": "doing",
            "queue_name": "marsupilami",
            "task_name": "mytask",
        },
        # We're taking this one.
        5: {
            "id": 5,
            "status": "doing",
            "queue_name": "marsupilami",
            "task_name": "mytask",
        },
        # And this one
        6: {
            "id": 6,
            "status": "doing",
            "queue_name": "marsupilami",
            "task_name": "mytask",
        },
    }
    connector.events = {
        1: [{"at": datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)}],
        2: [{"at": datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)}],
        3: [{"at": datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)}],
        4: [{"at": datetime.datetime(2100, 1, 1, tzinfo=datetime.timezone.utc)}],
        5: [{"at": datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)}],
        6: [{"at": datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)}],
    }

    results = connector.select_stalled_jobs_all(
        queue="marsupilami", task_name="mytask", nb_seconds=0
    )
    assert [job["id"] for job in results] == [5, 6]


def test_delete_old_jobs_run(connector):
    connector.jobs = {
        # We're not deleting this job because it's "doing"
        1: {"id": 1, "status": "doing", "queue_name": "marsupilami"},
        # This one because it's the wrong queue
        2: {"id": 2, "status": "succeeded", "queue_name": "other_queue"},
        # This one is not old enough
        3: {"id": 3, "status": "succeeded", "queue_name": "marsupilami"},
        # This one we delete
        4: {"id": 4, "status": "succeeded", "queue_name": "marsupilami"},
    }
    connector.events = {
        1: [
            {
                "type": "succeeded",
                "at": datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc),
            }
        ],
        2: [
            {
                "type": "succeeded",
                "at": datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc),
            }
        ],
        3: [{"type": "succeeded", "at": utils.utcnow()}],
        4: [
            {
                "type": "succeeded",
                "at": datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc),
            }
        ],
    }

    connector.delete_old_jobs_run(
        queue="marsupilami", statuses=("succeeded"), nb_hours=0
    )
    assert 4 not in connector.jobs


def test_fetch_job_one(connector):
    # This one will be selected, then skipped the second time because it's processing
    connector.defer_job_one(
        task_name="mytask",
        args={},
        queue="marsupilami",
        scheduled_at=None,
        lock="a",
        queueing_lock="a",
    )

    # This one because it's the wrong queue
    connector.defer_job_one(
        task_name="mytask",
        args={},
        queue="other_queue",
        scheduled_at=None,
        lock="b",
        queueing_lock="b",
    )
    # This one because of the scheduled_at
    connector.defer_job_one(
        task_name="mytask",
        args={},
        queue="marsupilami",
        scheduled_at=datetime.datetime(2100, 1, 1, tzinfo=datetime.timezone.utc),
        lock="c",
        queueing_lock="c",
    )
    # This one because of the lock
    connector.defer_job_one(
        task_name="mytask",
        args={},
        queue="marsupilami",
        scheduled_at=None,
        lock="a",
        queueing_lock="d",
    )
    # We're taking this one.
    connector.defer_job_one(
        task_name="mytask",
        args={},
        queue="marsupilami",
        scheduled_at=None,
        lock="e",
        queueing_lock="e",
    )

    assert connector.fetch_job_one(queues=["marsupilami"])["id"] == 1
    assert connector.fetch_job_one(queues=["marsupilami"])["id"] == 5


def test_fetch_job_one_none_lock(connector):
    """Testing that 2 jobs with locks "None" don't block one another"""
    connector.defer_job_one(
        task_name="mytask",
        args={},
        queue="default",
        scheduled_at=None,
        lock=None,
        queueing_lock=None,
    )
    connector.defer_job_one(
        task_name="mytask",
        args={},
        queue="default",
        scheduled_at=None,
        lock=None,
        queueing_lock=None,
    )

    assert connector.fetch_job_one(queues=None)["id"] == 1
    assert connector.fetch_job_one(queues=None)["id"] == 2


def test_finish_job_run(connector):
    connector.defer_job_one(
        task_name="mytask",
        args={},
        queue="marsupilami",
        scheduled_at=None,
        lock="sher",
        queueing_lock="houba",
    )
    job_row = connector.fetch_job_one(queues=None)
    id = job_row["id"]

    connector.finish_job_run(job_id=id, status="finished", delete_job=False)

    assert connector.jobs[id]["attempts"] == 1
    assert connector.jobs[id]["status"] == "finished"


def test_retry_job_run(connector):
    connector.defer_job_one(
        task_name="mytask",
        args={},
        queue="marsupilami",
        scheduled_at=None,
        lock="sher",
        queueing_lock="houba",
    )
    job_row = connector.fetch_job_one(queues=None)
    id = job_row["id"]

    retry_at = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
    connector.retry_job_run(job_id=id, retry_at=retry_at)

    assert connector.jobs[id]["attempts"] == 1
    assert connector.jobs[id]["status"] == "todo"
    assert connector.jobs[id]["scheduled_at"] == retry_at
    assert len(connector.events[id]) == 4


def test_apply_schema_run(connector):
    # If we don't crash, it's enough
    connector.apply_schema_run()


def test_listen_for_jobs_run(connector):
    # If we don't crash, it's enough
    connector.listen_for_jobs_run()


async def test_defer_no_notify(connector):
    # This test is there to check that if the deferred queue doesn't match the
    # listened queue, the testing connector doesn't notify.
    event = asyncio.Event()
    await connector.listen_notify(event=event, channels="some_other_channel")
    connector.defer_job_one(
        task_name="foo",
        lock="bar",
        args={},
        scheduled_at=None,
        queue="baz",
        queueing_lock="houba",
    )

    assert not event.is_set()
