"""Durable SQLite storage for trusted, pickleable CASPaxos acceptor state."""

import asyncio
import pickle
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from os import PathLike, fspath
from typing import Any, Callable, List, Optional, Tuple

from ..acceptor import AcceptorState
from ..backend import Result, StorageBackend


class SQLiteStorage(StorageBackend):
    """Persist one logical acceptor's state in a SQLite database file.

    The complete state is pickled, preserving Python values and unbounded
    ballot integers. Values must be pickleable, and database files must be
    trusted: loading a malicious pickle can execute arbitrary Python code.

    A dedicated worker owns the connection and performs all SQLite I/O.
    ``update_state`` also executes its synchronous transition in that worker,
    not the event-loop thread. Transitions must not depend on running in the
    event loop. ``set_state`` pickles its snapshot before yielding to prevent
    queued writes from observing subsequent caller mutations.

    WAL mode and synchronous FULL make acknowledged writes durable. Each
    atomic transition holds a BEGIN IMMEDIATE transaction, including across
    independent connections and processes accessing the same file.

    Cancelling an operation's waiter does not cancel its submitted work; a
    write may still commit. ``close`` rejects new work immediately and drains
    all submitted work before closing the connection and shutting down the
    worker. Cancelling a close waiter does not interrupt that cleanup; another
    call to ``close`` can await its completion. Always close the storage, or
    use it as an asynchronous context manager.
    """

    def __init__(self, path: str | PathLike[str], *, timeout: float = 5.0):
        """Configure storage without opening the database until first use."""
        self._path = fspath(path)
        self._timeout = timeout
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="caspaxos-sqlite"
        )
        self._connection: Optional[sqlite3.Connection] = None
        self._closed = False
        self._close_future: Optional[asyncio.Future[None]] = None

    @staticmethod
    def _observe_future(future: asyncio.Future) -> None:
        """Retrieve failures even when the shielded waiter was cancelled."""
        if not future.cancelled():
            future.exception()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("SQLiteStorage is closed")

    async def _run(self, operation: Callable[..., Result], *args: Any) -> Result:
        self._ensure_open()
        future = asyncio.get_running_loop().run_in_executor(
            self._executor, operation, *args
        )
        future.add_done_callback(self._observe_future)
        return await asyncio.shield(future)

    def _get_connection(self) -> sqlite3.Connection:
        """Open and configure the connection exclusively in its worker."""
        if self._connection is None:
            connection = sqlite3.connect(
                self._path, timeout=self._timeout, isolation_level=None
            )
            try:
                journal_mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()
                if journal_mode[0].lower() != "wal":
                    raise sqlite3.OperationalError(
                        "SQLiteStorage requires a database file supporting WAL mode"
                    )
                connection.execute("PRAGMA synchronous = FULL")
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS acceptor_state "
                    "(key TEXT PRIMARY KEY, state BLOB NOT NULL)"
                )
            except BaseException:
                connection.close()
                raise
            self._connection = connection
        return self._connection

    def _initialize(self) -> None:
        self._get_connection()

    async def __aenter__(self) -> "SQLiteStorage":
        try:
            await self._run(self._initialize)
        except BaseException:
            await self.close()
            raise
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.close()

    def _close_connection(self) -> None:
        try:
            if self._connection is not None:
                self._connection.close()
                self._connection = None
        finally:
            # Already last in this executor's queue. Do not join our own thread;
            # shutdown's sentinel makes it exit after this final job completes.
            self._executor.shutdown(wait=False)

    async def close(self) -> None:
        """Drain accepted work and close, even if this waiter is cancelled."""
        if self._close_future is None:
            self._closed = True
            self._close_future = asyncio.get_running_loop().run_in_executor(
                self._executor, self._close_connection
            )
            self._close_future.add_done_callback(self._observe_future)
        await asyncio.shield(self._close_future)

    def _get_state(self, key: str) -> Optional[AcceptorState]:
        row = self._get_connection().execute(
            "SELECT state FROM acceptor_state WHERE key = ?", (key,)
        ).fetchone()
        return None if row is None else pickle.loads(row[0])

    async def get_state(self, key: str) -> Optional[AcceptorState]:
        """Return a detached snapshot, or None if the key is absent."""
        return await self._run(self._get_state, key)

    def _set_state(self, key: str, snapshot: bytes) -> None:
        self._get_connection().execute(
            "INSERT INTO acceptor_state (key, state) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET state = excluded.state",
            (key, snapshot),
        )

    async def set_state(self, key: str, state: AcceptorState) -> None:
        """Snapshot before yielding, then durably write the serialized state."""
        self._ensure_open()
        snapshot = pickle.dumps(state, protocol=pickle.HIGHEST_PROTOCOL)
        await self._run(self._set_state, key, snapshot)

    def _update_state(
        self,
        key: str,
        transition: Callable[[Optional[AcceptorState]], Tuple[Optional[AcceptorState], Result]],
    ) -> Result:
        connection = self._get_connection()
        connection.execute("BEGIN IMMEDIATE")
        try:
            row = connection.execute(
                "SELECT state FROM acceptor_state WHERE key = ?", (key,)
            ).fetchone()
            state = None if row is None else pickle.loads(row[0])
            replacement, result = transition(state)
            if replacement is not None:
                snapshot = pickle.dumps(replacement, protocol=pickle.HIGHEST_PROTOCOL)
                connection.execute(
                    "INSERT INTO acceptor_state (key, state) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET state = excluded.state",
                    (key, snapshot),
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return result

    async def update_state(
        self,
        key: str,
        transition: Callable[[Optional[AcceptorState]], Tuple[Optional[AcceptorState], Result]],
    ) -> Result:
        """Run a synchronous transition in the worker and commit atomically.

        The input snapshot and any returned result cannot alias stored state.
        A None replacement does not write, even if the input was mutated.
        Transition and serialization failures roll back the transaction.
        """
        return await self._run(self._update_state, key, transition)

    def _delete_state(self, key: str) -> None:
        self._get_connection().execute(
            "DELETE FROM acceptor_state WHERE key = ?", (key,)
        )

    async def delete_state(self, key: str) -> None:
        """Delete a key, doing nothing if it is absent."""
        await self._run(self._delete_state, key)

    def _list_keys(self) -> List[str]:
        return [
            row[0]
            for row in self._get_connection().execute("SELECT key FROM acceptor_state")
        ]

    async def list_keys(self) -> List[str]:
        """Return all stored keys without deserializing their values."""
        return await self._run(self._list_keys)

    def _clear(self) -> None:
        self._get_connection().execute("DELETE FROM acceptor_state")

    async def clear(self) -> None:
        """Delete all stored state."""
        await self._run(self._clear)

    def _exists(self, key: str) -> bool:
        return self._get_connection().execute(
            "SELECT 1 FROM acceptor_state WHERE key = ?", (key,)
        ).fetchone() is not None

    async def exists(self, key: str) -> bool:
        """Check key presence without deserializing its value."""
        return await self._run(self._exists, key)
