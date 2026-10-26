"""Durability and transaction tests for the SQLite backend."""

import asyncio
import pickle
import sqlite3
import subprocess
import sys
import threading
from contextlib import AsyncExitStack
from pathlib import Path

import pytest

from ..acceptor import BaseAcceptor
from ..core.ballot import BallotNumber
from ..core.messages import AcceptRequest, PrepareRequest
from ..storage.acceptor import AcceptorState
from ..storage.backends import SQLiteStorage
from ..caspaxos import CASPaxos


@pytest.mark.asyncio
async def test_reopen_preserves_complete_state(tmp_path):
    path = tmp_path / "acceptor.sqlite"
    state = AcceptorState(
        BallotNumber(2**80, "proposer"),
        BallotNumber(2**80 - 1, "proposer"),
        {"items": [1], "types": (b"bytes", {2, 3})},
    )
    async with SQLiteStorage(path) as storage:
        await storage.set_state("key", state)
    async with SQLiteStorage(path) as storage:
        assert await storage.get_state("key") == state


@pytest.mark.asyncio
async def test_restarted_acceptor_retains_promises_and_accepted_value(tmp_path):
    path = tmp_path / "acceptor.sqlite"
    accepted = BallotNumber(4, "p")
    promised = BallotNumber(8, "q")
    async with SQLiteStorage(path) as storage:
        acceptor = BaseAcceptor("a", storage)
        assert (await acceptor.prepare(PrepareRequest("key", accepted))).is_prepared
        assert (await acceptor.accept(
            AcceptRequest("key", accepted, {"items": [1]}, accepted)
        )).is_ok
        assert (await acceptor.prepare(PrepareRequest("key", promised))).is_prepared

    async with SQLiteStorage(path) as storage:
        acceptor = BaseAcceptor("a", storage)
        stale = BallotNumber(7, "z")
        assert (await acceptor.prepare(PrepareRequest("key", stale))).is_conflict
        assert (await acceptor.accept(
            AcceptRequest("key", stale, "overwrite", stale)
        )).is_conflict
        response = await acceptor.prepare(PrepareRequest("key", BallotNumber(9, "p")))
        assert response.is_prepared
        assert response.ballot == accepted
        assert response.value == {"items": [1]}


@pytest.mark.asyncio
async def test_independent_connections_do_not_lose_updates(tmp_path):
    path = tmp_path / "acceptor.sqlite"
    async with SQLiteStorage(path) as first, SQLiteStorage(path) as second:
        def increment(state):
            state = state or AcceptorState.initial()
            state.accepted_value = (state.accepted_value or 0) + 1
            return state, state.accepted_value

        results = await asyncio.gather(*(
            storage.update_state("counter", increment)
            for _ in range(20)
            for storage in (first, second)
        ))
        assert sorted(results) == list(range(1, 41))
    async with SQLiteStorage(path) as storage:
        assert (await storage.get_state("counter")).accepted_value == 40


@pytest.mark.asyncio
async def test_acknowledged_value_survives_abrupt_cluster_exit(tmp_path):
    writer = """
import asyncio
import os
import sys
from pathlib import Path
from caspaxos import CASPaxos
from caspaxos.acceptor import BaseAcceptor
from caspaxos.core.ballot import BallotNumber
from caspaxos.core.messages import PrepareRequest
from caspaxos.storage.backends import SQLiteStorage

async def main():
    acceptors = [
        BaseAcceptor(str(i), SQLiteStorage(Path(sys.argv[1]) / f"{i}.sqlite"))
        for i in range(3)
    ]
    client = CASPaxos("p", acceptors=acceptors)
    assert await client.put("key", {"counter": 42}) == {"counter": 42}
    responses = await asyncio.gather(*(
        acceptor.prepare(PrepareRequest("fence", BallotNumber(100, "p")))
        for acceptor in acceptors
    ))
    assert all(response.is_prepared for response in responses)
    os._exit(23)  # No connection close, executor shutdown, or WAL checkpoint.

asyncio.run(main())
"""
    process = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-c", writer, str(tmp_path)],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert process.returncode == 23, process.stderr
    async with AsyncExitStack() as stack:
        stores = [
            await stack.enter_async_context(SQLiteStorage(tmp_path / f"{i}.sqlite"))
            for i in range(3)
        ]
        acceptors = [
            BaseAcceptor(str(i), storage) for i, storage in enumerate(stores)
        ]
        stale = BallotNumber(99, "p")
        for acceptor in acceptors:
            assert (await acceptor.prepare(PrepareRequest("fence", stale))).is_conflict
            assert (await acceptor.accept(
                AcceptRequest("fence", stale, "overwrite", stale)
            )).is_conflict
        client = CASPaxos("p", acceptors=acceptors)
        assert await client.get("key") == {"counter": 42}
        assert await client.change(
            "key", lambda value: {"counter": value["counter"] + 1}
        ) == {"counter": 43}


@pytest.mark.asyncio
async def test_database_lock_does_not_block_event_loop(tmp_path):
    path = tmp_path / "acceptor.sqlite"
    async with SQLiteStorage(path, timeout=1.0) as storage:
        blocker = sqlite3.connect(path, isolation_level=None)
        blocker.execute("BEGIN IMMEDIATE")
        state = AcceptorState(BallotNumber(1, "p"), BallotNumber.zero(), "saved")
        pending = asyncio.create_task(storage.set_state("key", state))
        try:
            done, _ = await asyncio.wait([pending], timeout=0.05)
            assert not done, "database lock wait blocked the event loop until timeout"
        finally:
            blocker.rollback()
            blocker.close()
            await asyncio.gather(pending, return_exceptions=True)
        await pending
        assert await storage.get_state("key") == state


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["set", "transition"])
async def test_serialization_failure_preserves_state_and_releases_transaction(tmp_path, operation):
    class Unserializable:
        def __reduce__(self):
            raise pickle.PicklingError("cannot serialize")

    async with SQLiteStorage(tmp_path / "acceptor.sqlite") as storage:
        initial = AcceptorState(BallotNumber(1, "p"), BallotNumber(1, "p"), 1)
        await storage.set_state("key", initial)

        def invalid(state):
            state.promised_ballot.counter = 99
            state.accepted_value = Unserializable()
            return state, None

        with pytest.raises(pickle.PicklingError):
            if operation == "set":
                await storage.set_state("key", invalid(await storage.get_state("key"))[0])
            else:
                await storage.update_state("key", invalid)
        assert await storage.get_state("key") == initial

        def valid(state):
            state.accepted_value += 1
            return state, state.accepted_value

        assert await storage.update_state("key", valid) == 2
        assert (await storage.get_state("key")).accepted_value == 2


@pytest.mark.asyncio
async def test_cancelled_transaction_and_close_release_database(tmp_path):
    path = tmp_path / "acceptor.sqlite"
    storage = SQLiteStorage(path)
    started = threading.Event()
    release = threading.Event()
    initial = AcceptorState(BallotNumber.zero(), BallotNumber.zero(), 0)
    replacement = AcceptorState(BallotNumber(1, "p"), BallotNumber(1, "p"), 1)
    await storage.set_state("key", initial)

    def delayed(state):
        started.set()
        if not release.wait(5):
            raise TimeoutError("test did not release transaction")
        return replacement, None

    pending = asyncio.create_task(storage.update_state("key", delayed))
    closing = None
    try:
        assert await asyncio.to_thread(started.wait, 2)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        closing = asyncio.create_task(storage.close())
        done, _ = await asyncio.wait([closing], timeout=0.05)
        assert not done, "close returned before the running transaction finished"
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
    finally:
        release.set()
        await asyncio.gather(pending, return_exceptions=True)
        if closing is not None:
            await asyncio.gather(closing, return_exceptions=True)
        await storage.close()

    with pytest.raises(RuntimeError):
        await storage.get_state("key")
    async with SQLiteStorage(path) as reopened:
        assert await reopened.get_state("key") in (initial, replacement)
        await reopened.set_state("key", initial)
        assert await reopened.get_state("key") == initial


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["delete", "clear"])
async def test_removals_survive_reopen(tmp_path, operation):
    path = tmp_path / "acceptor.sqlite"
    async with SQLiteStorage(path) as storage:
        await storage.set_state("removed", AcceptorState.initial())
        await storage.set_state("retained", AcceptorState.initial())
        if operation == "delete":
            await storage.delete_state("removed")
        else:
            await storage.clear()
    async with SQLiteStorage(path) as storage:
        assert await storage.get_state("removed") is None
        if operation == "delete":
            assert await storage.get_state("retained") == AcceptorState.initial()
            assert set(await storage.list_keys()) == {"retained"}
        else:
            assert await storage.get_state("retained") is None
            assert await storage.list_keys() == []


@pytest.mark.asyncio
async def test_keys_are_bound_as_data(tmp_path):
    async with SQLiteStorage(tmp_path / "acceptor.sqlite") as storage:
        key = "'; DROP TABLE acceptor_state; --"
        state = AcceptorState(BallotNumber(2, "p"), BallotNumber(1, "p"), "saved")
        await storage.set_state("retained", AcceptorState.initial())
        await storage.set_state(key, state)
        assert await storage.get_state(key) == state
        await storage.delete_state(key)
        assert await storage.get_state(key) is None
        assert await storage.get_state("retained") == AcceptorState.initial()
