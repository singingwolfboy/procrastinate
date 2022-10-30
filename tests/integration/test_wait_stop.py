import asyncio

import pytest

from procrastinate import app
from procrastinate import worker as worker_module


async def test_wait_for_activity(async_connector):
    """
    Testing that a new event interrupts the wait
    """
    pg_app = app.App(connector=async_connector)
    worker = worker_module.Worker(app=pg_app, timeout=2)
    worker.notify_event = asyncio.Event()
    task = asyncio.ensure_future(worker.single_worker(worker_id=0))
    await asyncio.sleep(0.2)  # should be enough so that we're waiting

    worker.stop_requested = True
    worker.notify_event.set()

    try:
        await asyncio.wait_for(task, timeout=0.2)
    except asyncio.TimeoutError:
        pytest.fail("Failed to stop worker within .2s")


async def test_wait_for_activity_timeout(async_connector):
    """
    Testing that we timeout if nothing happens
    """
    pg_app = app.App(connector=async_connector)
    worker = worker_module.Worker(app=pg_app, timeout=2)
    worker.notify_event = asyncio.Event()
    task = asyncio.ensure_future(worker.single_worker(worker_id=0))
    try:
        await asyncio.sleep(0.2)  # should be enough so that we're waiting

        worker.stop_requested = True

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(task, timeout=0.2)
    finally:
        worker.notify_event.set()


async def test_wait_for_activity_stop_from_signal(async_connector, kill_own_pid):
    """
    Testing than ctrl+c interrupts the wait
    """
    pg_app = app.App(connector=async_connector)
    worker = worker_module.Worker(app=pg_app, timeout=2)
    task = asyncio.ensure_future(worker.run())
    await asyncio.sleep(0.2)  # should be enough so that we're waiting

    kill_own_pid()

    try:
        await asyncio.wait_for(task, timeout=0.2)
    except asyncio.TimeoutError:
        pytest.fail("Failed to stop worker within .2s")


async def test_wait_for_activity_stop(async_connector):
    """
    Testing than calling job_manager.stop() interrupts the wait
    """
    pg_app = app.App(connector=async_connector)
    worker = worker_module.Worker(app=pg_app, timeout=2)
    task = asyncio.ensure_future(worker.run())
    await asyncio.sleep(0.2)  # should be enough so that we're waiting

    worker.stop()

    try:
        await asyncio.wait_for(task, timeout=0.2)
    except asyncio.TimeoutError:
        pytest.fail("Failed to stop worker within .2s")
