import datetime

import pytest

from procrastinate import tasks, utils

tz1 = datetime.timezone(datetime.timedelta(hours=1))


def task_func():
    pass


def test_task_init_with_no_name(app):
    task = tasks.Task(task_func, blueprint=app, queue="queue")

    assert task.func is task_func
    assert task.name == "tests.unit.test_tasks.task_func"


async def test_task_defer_async(app, connector):
    task = tasks.Task(task_func, blueprint=app, queue="queue")

    await task.defer_async(c=3)

    # The lock is the only thing we can't predict
    lock = connector.jobs[1]["lock"]
    assert connector.jobs == {
        1: {
            "id": 1,
            "queue_name": "queue",
            "task_name": "tests.unit.test_tasks.task_func",
            "lock": lock,
            "queueing_lock": None,
            "args": {"c": 3},
            "status": "todo",
            "scheduled_at": None,
            "attempts": 0,
        }
    }


def test_configure_task(job_manager):
    job = tasks.configure_task(
        name="my_name", job_manager=job_manager, lock="sher", task_kwargs={"yay": "ho"}
    ).job

    assert job.lock == "sher"
    assert job.task_kwargs == {"yay": "ho"}


def test_configure_task_schedule_at(job_manager):
    job = tasks.configure_task(
        name="my_name",
        job_manager=job_manager,
        schedule_at=datetime.datetime(2000, 1, 1, tzinfo=tz1),
    ).job

    assert job.scheduled_at == datetime.datetime(2000, 1, 1, tzinfo=tz1)


def test_configure_task_schedule_in(job_manager, mocker):
    now = datetime.datetime(2000, 1, 1, tzinfo=tz1)
    mocker.patch.object(utils, "utcnow", return_value=now)
    job = tasks.configure_task(
        name="my_name", job_manager=job_manager, schedule_in={"hours": 2}
    ).job

    assert job.scheduled_at == datetime.datetime(2000, 1, 1, 2, tzinfo=tz1)


def test_configure_task_schedule_in_and_schedule_at(job_manager):
    with pytest.raises(ValueError):
        tasks.configure_task(
            name="my_name",
            job_manager=job_manager,
            schedule_at=datetime.datetime(2000, 1, 1, tzinfo=tz1),
            schedule_in={"hours": 2},
        )


def test_task_configure(app):
    task = tasks.Task(task_func, blueprint=app, queue="queue")

    job = task.configure(lock="sher", task_kwargs={"yay": "ho"}).job

    assert job.task_name == "tests.unit.test_tasks.task_func"
    assert job.lock == "sher"
    assert job.task_kwargs == {"yay": "ho"}
    assert job.queue == "queue"


def test_task_configure_override_queue(app):
    task = tasks.Task(task_func, blueprint=app, queue="queue")

    job = task.configure(queue="other_queue").job

    assert job.queue == "other_queue"


def test_task_get_retry_exception_none(app):
    task = tasks.Task(task_func, blueprint=app, queue="queue")
    job = task.configure().job

    assert task.get_retry_exception(exception=None, job=job) is None


def test_task_get_retry_exception(app, mocker):
    mock = mocker.patch("procrastinate.retry.RetryStrategy.get_retry_exception")

    task = tasks.Task(task_func, blueprint=app, queue="queue", retry=10)
    job = task.configure().job

    exception = ValueError()
    assert task.get_retry_exception(exception=exception, job=job) is mock.return_value
    mock.assert_called_with(exception=exception, attempts=0)
