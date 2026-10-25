"""Quorum progress, deadlines, and request cancellation regressions."""

import asyncio

import pytest

from ..core.proposer import Proposer, ProposerError
from ..core.proposer_enhanced import EnhancedProposer, ProposerError as EnhancedProposerError
from ..storage.memory import InMemoryAcceptor


@pytest.fixture(params=[Proposer, EnhancedProposer])
def proposer_type(request):
    return request.param


def change_once(proposer, update_fn):
    if isinstance(proposer, EnhancedProposer):
        return proposer.change("key", update_fn, retries=1)
    return proposer.change("key", update_fn)


class StalledAcceptor(InMemoryAcceptor):
    def __init__(self, node_id, phase, error=None):
        super().__init__(node_id)
        self.phase = phase
        self.error = error
        self.started = asyncio.Event()
        self.stopped = asyncio.Event()
        self.release = asyncio.Event()

    async def pause(self, phase):
        if phase != self.phase:
            return
        self.started.set()
        try:
            if self.error is not None:
                raise self.error
            await self.release.wait()
        finally:
            self.stopped.set()

    async def prepare(self, request):
        await self.pause("prepare")
        return await super().prepare(request)

    async def accept(self, request):
        await self.pause("accept")
        return await super().accept(request)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["prepare", "accept"])
async def test_healthy_quorum_does_not_wait_for_stalled_minority(proposer_type, phase):
    stalled = StalledAcceptor("stalled", phase)
    healthy = [InMemoryAcceptor("a"), InMemoryAcceptor("b")]
    proposer = proposer_type("p", [*healthy, stalled])
    assert await asyncio.wait_for(change_once(proposer, lambda _: "committed"), 0.5) == "committed"
    await asyncio.wait_for(stalled.stopped.wait(), 0.5)
    for acceptor in healthy:
        assert (await acceptor.get_state("key")).accepted_value == "committed"


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["prepare", "accept"])
async def test_phase_deadline_fails_when_a_quorum_stalls(proposer_type, phase):
    stalled = [StalledAcceptor(str(i), phase) for i in range(2)]
    proposer = proposer_type("p", [InMemoryAcceptor("healthy"), *stalled], phase_timeout=0.02)
    error_type = EnhancedProposerError if proposer_type is EnhancedProposer else ProposerError
    with pytest.raises(error_type) as error:
        await asyncio.wait_for(change_once(proposer, lambda value: (value or 0) + 1), 0.5)
    if proposer_type is EnhancedProposer and phase == "accept":
        assert error.value.code == "INDETERMINATE"
    for acceptor in stalled:
        await asyncio.wait_for(acceptor.stopped.wait(), 0.5)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["prepare", "accept"])
async def test_impossible_quorum_does_not_wait_for_remaining_call(proposer_type, phase):
    stalled = StalledAcceptor("stalled", phase)
    acceptors = [
        StalledAcceptor("failed", phase, ConnectionError("unavailable")),
        StalledAcceptor("cancelled", phase, asyncio.CancelledError()),
        stalled,
    ]
    proposer = proposer_type("p", acceptors)
    error_type = EnhancedProposerError if proposer_type is EnhancedProposer else ProposerError
    with pytest.raises(error_type):
        await asyncio.wait_for(change_once(proposer, lambda _: "value"), 0.5)
    await asyncio.wait_for(stalled.stopped.wait(), 0.5)


@pytest.mark.asyncio
async def test_caller_cancellation_stops_outstanding_requests(proposer_type):
    acceptors = [StalledAcceptor(str(i), "prepare") for i in range(3)]
    proposer = proposer_type("p", acceptors)
    pending = asyncio.create_task(change_once(proposer, lambda _: "value"))
    try:
        await asyncio.wait_for(asyncio.gather(*(a.started.wait() for a in acceptors)), 0.5)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        await asyncio.wait_for(asyncio.gather(*(a.stopped.wait() for a in acceptors)), 0.5)
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.asyncio
async def test_late_accept_does_not_block_quorum_or_overwrite_newer_value(proposer_type):
    release = asyncio.Event()
    cancelled = asyncio.Event()
    finished = asyncio.Event()

    class SlowCancellationAcceptor(InMemoryAcceptor):
        first_accept = True

        async def accept(self, request):
            if not self.first_accept:
                return await super().accept(request)
            self.first_accept = False
            try:
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    cancelled.set()
                    await release.wait()
                return await super().accept(request)
            finally:
                finished.set()

    slow = SlowCancellationAcceptor("slow")
    proposer = proposer_type("p", [InMemoryAcceptor("a"), InMemoryAcceptor("b"), slow])
    pending = asyncio.create_task(change_once(proposer, lambda _: 1))
    try:
        # Do not use wait_for here: it waits for cancellation and could itself
        # hang on the old implementation's cancellation-resistant request.
        done, _ = await asyncio.wait([pending], timeout=0.5)
        assert pending in done, "a healthy quorum waited for the slow replica"
        assert await pending == 1
        await asyncio.wait_for(cancelled.wait(), 0.5)
        assert await asyncio.wait_for(change_once(proposer, lambda value: value + 1), 0.5) == 2
        release.set()
        await asyncio.wait_for(finished.wait(), 0.5)
        assert (await slow.get_state("key")).accepted_value == 2
    finally:
        release.set()
        await asyncio.gather(pending, return_exceptions=True)
