"""CASPaxos proposer for local, remote, and mixed acceptor clusters."""

import asyncio
from copy import deepcopy
from math import isfinite
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from .ballot import BallotNumber
from .messages import PrepareRequest, PrepareResponse, AcceptRequest, AcceptResponse
from .quorum import collect_responses
from ..storage.acceptor import Acceptor
from ..transport.base import Transport
from ..transport.proxy import RemoteAcceptor


class ProposerError(Exception):
    """A failed preparation, indeterminate accept, or rejected update."""

    def __init__(self, code: str, message: str = ""):
        super().__init__(message or code)
        self.code = code


class InsufficientQuorumError(Exception):
    """A protocol phase could not confirm enough successful responses."""

    def __init__(self, responses: List[Any]):
        super().__init__(f"Insufficient quorum: {len(responses)} responses")
        self.responses = responses


class Proposer:
    """Coordinate consensus using one API for every acceptor configuration.

    Updates on the same key are serialized locally. Each operation prepares a
    fresh ballot so writes by other proposers are observed before applying the
    update. Writes retry preparation only; reads may also retry failed accepts.
    """

    def __init__(
        self,
        node_id: str,
        acceptors: Optional[List[Acceptor]] = None,
        quorum_size: Optional[int] = None,
        phase_timeout: float = 5.0,
        *,
        transport: Optional[Transport] = None,
        remote_acceptor_ids: Optional[List[str]] = None,
    ):
        """Configure local acceptors, remote IDs, or both.

        quorum_size defaults to a majority of the combined cluster. Each phase
        has its own positive, finite phase_timeout in seconds. The caller owns
        the transport and remains responsible for closing it.
        """
        if remote_acceptor_ids and transport is None:
            raise ValueError("remote_acceptor_ids require a transport")
        self.node_id = node_id
        self.transport = transport
        self.acceptors = list(acceptors or [])
        if transport is not None:
            self.acceptors.extend(
                RemoteAcceptor(acceptor_id, transport)
                for acceptor_id in remote_acceptor_ids or []
            )
        if not self.acceptors:
            raise ValueError("No acceptors provided (neither local nor remote)")
        self.quorum_size = quorum_size or (len(self.acceptors) // 2 + 1)
        if self.quorum_size > len(self.acceptors):
            raise ValueError(
                f"Quorum size {self.quorum_size} > acceptors {len(self.acceptors)}"
            )
        if not isfinite(phase_timeout) or phase_timeout <= 0:
            raise ValueError("phase_timeout must be positive and finite")
        self.phase_timeout = phase_timeout
        self.ballot_counters: Dict[str, int] = {}
        self.locks: Dict[str, asyncio.Lock] = {}

    def _get_lock(self, key: str) -> asyncio.Lock:
        if key not in self.locks:
            self.locks[key] = asyncio.Lock()
        return self.locks[key]

    def _next_ballot(self, key: str) -> BallotNumber:
        counter = self.ballot_counters.get(key, 0) + 1
        self.ballot_counters[key] = counter
        return BallotNumber(counter, self.node_id)

    async def change(
        self,
        key: str,
        update_fn: Callable[[Optional[Any]], Any],
        extra: Optional[Any] = None,
        *,
        retries: int = 10,
    ) -> Any:
        """Apply an update once, after obtaining a current value and ballot.

        retries bounds prepare attempts, not accept attempts. An unconfirmed
        accept raises ProposerError with code INDETERMINATE and is never replayed.
        Exhausted preparation raises MAX_RETRIES. A callback exception commits
        the unchanged value, then raises UpdateError with the original cause.
        Other unexpected errors use UnexpectedError; cancellation propagates.

        Values are isolated snapshots. extra is forwarded to protocol requests.
        """
        return await self._change(key, update_fn, extra, retries)

    async def _change(
        self,
        key: str,
        update_fn: Callable[[Optional[Any]], Any],
        extra: Optional[Any],
        retries: int,
        *,
        retry_accept: bool = False,
    ) -> Any:
        """Run one shared attempt budget; only identity reads retry acceptance."""
        async with self._get_lock(key):
            try:
                last_error = None
                for attempt in range(retries):
                    try:
                        ballot, current_value = await self._prepare_phase(key, extra)
                    except InsufficientQuorumError as error:
                        last_error = error
                    else:
                        update_error = None
                        try:
                            new_value = update_fn(deepcopy(current_value))
                        except Exception as error:
                            update_error = error
                            new_value = current_value
                        # Snapshot before request I/O can let callers mutate the result.
                        new_value = deepcopy(new_value)
                        try:
                            await self._accept_phase(key, ballot, new_value, extra)
                        except InsufficientQuorumError as error:
                            if not retry_accept:
                                # Even a constant write may have been observed
                                # before another writer replaced it.
                                raise ProposerError(
                                    "INDETERMINATE",
                                    "Accept quorum was not confirmed; the update may have been applied",
                                ) from error
                            last_error = error
                        else:
                            if update_error is not None:
                                raise ProposerError("UpdateError", str(update_error)) from update_error
                            return deepcopy(new_value)

                    if attempt + 1 < retries:
                        conflicted = any(
                            getattr(response, "is_conflict", False)
                            for response in last_error.responses
                        )
                        delay = 0.01 * (2 ** attempt) if conflicted else 0.1
                        await asyncio.sleep(delay)
                raise ProposerError(
                    "MAX_RETRIES",
                    f"Failed after {retries} attempts. Last error: {last_error}",
                ) from last_error
            except ProposerError:
                raise
            except Exception as error:
                raise ProposerError("UnexpectedError", str(error)) from error

    async def _prepare_phase(
        self, key: str, extra: Optional[Any] = None
    ) -> Tuple[BallotNumber, Optional[Any]]:
        # A round-local ballot cannot be changed by work on another key.
        ballot = self._next_ballot(key)
        responses = await self._wait_for_quorum(
            key,
            [acceptor.prepare(PrepareRequest(key, ballot, extra)) for acceptor in self.acceptors],
            lambda response: isinstance(response, PrepareResponse) and response.is_prepared,
        )
        highest_ballot = BallotNumber.zero()
        current_value = None
        for response in responses:
            if response.ballot is not None and response.ballot > highest_ballot:
                highest_ballot = response.ballot
                current_value = response.value
        return ballot, current_value

    async def _accept_phase(
        self,
        key: str,
        ballot: BallotNumber,
        value: Any,
        extra: Optional[Any] = None,
    ) -> None:
        await self._wait_for_quorum(
            key,
            [
                acceptor.accept(AcceptRequest(key, ballot, value, ballot, extra))
                for acceptor in self.acceptors
            ],
            lambda response: isinstance(response, AcceptResponse) and response.is_ok,
        )

    async def _wait_for_quorum(
        self,
        key: str,
        requests: List[Awaitable[Any]],
        success_predicate: Callable[[Any], bool],
    ) -> List[Any]:
        responses = await collect_responses(
            requests, self.quorum_size, success_predicate, self.phase_timeout
        )
        successful = []
        for response in responses:
            if isinstance(response, (PrepareResponse, AcceptResponse)):
                if response.is_conflict and response.ballot is not None:
                    self.ballot_counters[key] = max(
                        self.ballot_counters.get(key, 0), response.ballot.counter
                    )
                if success_predicate(response):
                    successful.append(response)
        if len(successful) < self.quorum_size:
            raise InsufficientQuorumError(responses)
        return successful

    async def get(self, key: str, *, retries: int = 10) -> Optional[Any]:
        """Read through consensus, retrying failed phases with a fresh prepare.

        retries bounds total prepare/accept attempts, not separate nested budgets.
        Exhaustion raises MAX_RETRIES, not an indeterminate write outcome.
        """
        return await self._change(
            key, lambda value: value, None, retries, retry_accept=True
        )

    async def put(self, key: str, value: Any) -> Any:
        """Replace a value; unconfirmed accepts raise INDETERMINATE without replay."""
        return await self.change(key, lambda _: value)

    async def compare_and_swap(
        self, key: str, expected: Any, new_value: Any
    ) -> Tuple[bool, Any]:
        """Return (matched, resulting_value) after consensus.

        matched reports whether the current value equaled expected, even when
        new_value already equals the current value. A mismatch leaves it unchanged.
        """
        matched = False

        def update(current):
            nonlocal matched
            matched = bool(current == expected)
            return new_value if matched else current

        actual_value = await self.change(key, update)
        return matched, actual_value
