"""Quorum progress, deadlines, and request cancellation regressions."""

import asyncio

import pytest

from ..core.proposer import Proposer, ProposerError
from ..storage.memory import InMemoryAcceptor


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
async def test_healthy_quorum_does_not_wait_for_stalled_minority(phase):
    stalled = StalledAcceptor("stalled", phase)
    healthy = [InMemoryAcceptor("a"), InMemoryAcceptor("b")]
    proposer = Proposer("p", [*healthy, stalled])
    assert await asyncio.wait_for(
        proposer.change("key", lambda _: "committed", retries=1), 0.5
    ) == "committed"
    await asyncio.wait_for(stalled.stopped.wait(), 0.5)
    for acceptor in healthy:
        assert (await acceptor.get_state("key")).accepted_value == "committed"


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["prepare", "accept"])
async def test_phase_deadline_fails_when_a_quorum_stalls(phase):
    stalled = [StalledAcceptor(str(i), phase) for i in range(2)]
    proposer = Proposer("p", [InMemoryAcceptor("healthy"), *stalled], phase_timeout=0.02)
    with pytest.raises(ProposerError) as error:
        await asyncio.wait_for(
            proposer.change("key", lambda value: (value or 0) + 1, retries=1), 0.5
        )
    assert error.value.code == ("INDETERMINATE" if phase == "accept" else "MAX_RETRIES")
    for acceptor in stalled:
        await asyncio.wait_for(acceptor.stopped.wait(), 0.5)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["prepare", "accept"])
async def test_impossible_quorum_does_not_wait_for_remaining_call(phase):
    stalled = StalledAcceptor("stalled", phase)
    acceptors = [
        StalledAcceptor("failed", phase, ConnectionError("unavailable")),
        StalledAcceptor("cancelled", phase, asyncio.CancelledError()),
        stalled,
    ]
    proposer = Proposer("p", acceptors)
    with pytest.raises(ProposerError) as error:
        await asyncio.wait_for(
            proposer.change("key", lambda _: "value", retries=1), 0.5
        )
    assert error.value.code == ("INDETERMINATE" if phase == "accept" else "MAX_RETRIES")
    await asyncio.wait_for(stalled.stopped.wait(), 0.5)


@pytest.mark.asyncio
async def test_caller_cancellation_stops_outstanding_requests():
    acceptors = [StalledAcceptor(str(i), "prepare") for i in range(3)]
    proposer = Proposer("p", acceptors)
    pending = asyncio.create_task(proposer.change("key", lambda _: "value", retries=1))
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
async def test_late_accept_does_not_block_quorum_or_overwrite_newer_value():
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
    proposer = Proposer("p", [InMemoryAcceptor("a"), InMemoryAcceptor("b"), slow])
    pending = asyncio.create_task(proposer.change("key", lambda _: 1, retries=1))
    try:
        # Do not use wait_for here: it waits for cancellation and could itself
        # hang on the old implementation's cancellation-resistant request.
        done, _ = await asyncio.wait([pending], timeout=0.5)
        assert pending in done, "a healthy quorum waited for the slow replica"
        assert await pending == 1
        await asyncio.wait_for(cancelled.wait(), 0.5)
        assert await asyncio.wait_for(
            proposer.change("key", lambda value: value + 1, retries=1), 0.5
        ) == 2
        release.set()
        await asyncio.wait_for(finished.wait(), 0.5)
        assert (await slow.get_state("key")).accepted_value == 2
    finally:
        release.set()
        await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["prepare", "accept"])
async def test_read_deadline_exhaustion_reports_no_result(phase):
    acceptors = [StalledAcceptor(str(i), phase) for i in range(3)]
    reader = Proposer("reader", acceptors, phase_timeout=0.02)
    with pytest.raises(ProposerError) as error:
        await asyncio.wait_for(reader.get("key", retries=1), 0.5)
    assert error.value.code == "MAX_RETRIES"
    for acceptor in acceptors:
        await asyncio.wait_for(acceptor.stopped.wait(), 0.5)


@pytest.mark.asyncio
async def test_read_cancellation_stops_retried_accept_requests():
    class RetryThenStallAcceptor(StalledAcceptor):
        first_accept = True

        async def accept(self, request):
            if self.first_accept:
                self.first_accept = False
                raise ConnectionError("first attempt unavailable")
            return await super().accept(request)

    acceptors = [RetryThenStallAcceptor(str(i), "accept") for i in range(3)]
    reader = Proposer("reader", acceptors)
    pending = asyncio.create_task(reader.get("key", retries=3))
    try:
        await asyncio.wait_for(
            asyncio.gather(*(acceptor.started.wait() for acceptor in acceptors)), 1
        )
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        await asyncio.wait_for(
            asyncio.gather(*(acceptor.stopped.wait() for acceptor in acceptors)), 0.5
        )
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
