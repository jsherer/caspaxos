"""Tests for Proposer implementation."""

import pytest
import asyncio
from ..core import Proposer, ProposerError
from ..core.ballot import BallotNumber
from ..storage.memory import InMemoryAcceptor
from ..core.messages import PrepareRequest


class TestProposer:
    """Test suite for Proposer."""
    
    @pytest.mark.asyncio
    async def test_basic_consensus(self):
        """Test basic consensus with majority of acceptors."""
        # Create 3 acceptors
        acceptors = [InMemoryAcceptor(f"acceptor_{i}") for i in range(3)]
        
        # Create proposer (needs 2 out of 3 for quorum)
        proposer = Proposer("proposer1", acceptors)
        
        # First change should succeed
        result = await proposer.change("key1", lambda x: "value1")
        assert result == "value1"
        
        # Second change should see previous value
        result = await proposer.change("key1", lambda x: f"{x}_modified")
        assert result == "value1_modified"
    
    
    @pytest.mark.asyncio
    async def test_concurrent_proposals_same_key(self):
        """Test that concurrent proposals on same key are serialized."""
        acceptors = [InMemoryAcceptor(f"acceptor_{i}") for i in range(3)]
        proposer = Proposer("proposer1", acceptors)
        
        # Initialize counter
        await proposer.change("counter", lambda x: 0)
        
        # Try concurrent increments
        async def increment():
            return await proposer.change("counter", lambda x: x + 1)
        
        # These should be serialized by the per-key lock
        tasks = [increment() for _ in range(10)]
        results = await asyncio.gather(*tasks)
        
        # Results should be sequential increments
        assert sorted(results) == list(range(1, 11))
        
        # Final value should be 10
        final = await proposer.change("counter", lambda x: x)
        assert final == 10
    
    @pytest.mark.asyncio
    async def test_concurrent_proposals_different_keys(self):
        """Test that proposals on different keys can run concurrently."""
        acceptors = [InMemoryAcceptor(f"acceptor_{i}") for i in range(3)]
        proposer = Proposer("proposer1", acceptors)
        
        async def set_value(key, value):
            return await proposer.change(key, lambda x: value)
        
        # These should run concurrently
        tasks = [
            set_value(f"key{i}", f"value{i}")
            for i in range(10)
        ]
        results = await asyncio.gather(*tasks)
        
        # Verify all succeeded
        assert results == [f"value{i}" for i in range(10)]
        
        # Verify values are correct
        for i in range(10):
            value = await proposer.change(f"key{i}", lambda x: x)
            assert value == f"value{i}"
    
    @pytest.mark.asyncio
    async def test_update_function_error(self):
        """Test handling of errors in update function."""
        acceptors = [InMemoryAcceptor(f"acceptor_{i}") for i in range(3)]
        proposer = Proposer("proposer1", acceptors)
        
        # Set initial value
        await proposer.change("key1", lambda x: "initial")
        
        # Update function that raises error
        def bad_update(x):
            if x == "initial":
                raise ValueError("Cannot update initial value")
            return "updated"
        
        # Should raise ProposerError
        with pytest.raises(ProposerError) as exc_info:
            await proposer.change("key1", bad_update)
        assert exc_info.value.code == "UpdateError"
        
        # Value should remain unchanged
        value = await proposer.change("key1", lambda x: x)
        assert value == "initial"
    
    def test_invalid_quorum(self):
        """Reject a quorum larger than the configured cluster."""
        acceptors = [InMemoryAcceptor(f"acceptor_{i}") for i in range(3)]
        with pytest.raises(ValueError):
            Proposer("proposer1", acceptors, quorum_size=4)

    @pytest.mark.asyncio
    async def test_other_keys_cannot_change_an_inflight_ballot(self):
        prepared = asyncio.Event()
        release = asyncio.Event()
        seen = set()

        class DelayedAcceptor(InMemoryAcceptor):
            async def prepare(self, request):
                response = await super().prepare(request)
                if request.key == "counter" and request.ballot.node_id == "p":
                    seen.add(self.node_id)
                    if len(seen) == 3:
                        prepared.set()
                    await release.wait()
                return response

        acceptors = [DelayedAcceptor(str(i)) for i in range(3)]
        proposer = Proposer("p", acceptors)
        competitor = Proposer("z", acceptors)
        pending = asyncio.create_task(
            proposer.change("counter", lambda value: (value or 0) + 1)
        )
        try:
            await asyncio.wait_for(prepared.wait(), 1)
            await competitor.change("counter", lambda _: 10)
            for key in ("other1", "other2", "other3"):
                await proposer.change(key, lambda _: "unrelated")
            release.set()
            with pytest.raises(ProposerError):
                await pending
            assert await competitor.change("counter", lambda value: value) == 10
        finally:
            release.set()
            await asyncio.gather(pending, return_exceptions=True)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("failure", ["connection_error", "cancellation"])
    async def test_partial_accept_retry_preserves_acknowledged_write(self, failure):
        accepted = asyncio.Event()
        blocked = asyncio.Event()

        class FaultAcceptor(InMemoryAcceptor):
            fail = False

            async def accept(self, request):
                if self.fail:
                    if failure == "cancellation":
                        await blocked.wait()
                    raise OSError("connection lost")
                response = await super().accept(request)
                accepted.set()
                return response

        acceptors = [FaultAcceptor(str(i)) for i in range(3)]
        proposer = Proposer("p", acceptors)
        acceptors[1].fail = acceptors[2].fail = True
        pending = asyncio.create_task(proposer.change("key", lambda _: "first"))
        try:
            await asyncio.wait_for(accepted.wait(), 1)
            if failure == "cancellation":
                pending.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await pending
            else:
                with pytest.raises(ProposerError) as error:
                    await pending
                assert error.value.code == "INDETERMINATE"
        finally:
            if not pending.done():
                pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)

        acceptors[1].fail = acceptors[2].fail = False
        assert await proposer.change("key", lambda _: "second") == "second"

        # A fresh proposer must recover the acknowledged value, not the
        # minority's earlier value from an ambiguously completed operation.
        reader = Proposer("z", acceptors)
        value = await reader.get("key")
        assert value == "second"

    @pytest.mark.asyncio
    async def test_proposal_snapshot_precedes_accept_io(self):
        accepting = asyncio.Event()
        release = asyncio.Event()

        class DelayedAcceptor(InMemoryAcceptor):
            async def accept(self, request):
                accepting.set()
                await release.wait()
                return await super().accept(request)

        acceptors = [DelayedAcceptor(str(i)) for i in range(3)]
        proposer = Proposer("p", acceptors)
        original = {"items": [1]}
        pending = asyncio.create_task(proposer.change("key", lambda _: original))
        try:
            await asyncio.wait_for(accepting.wait(), 1)
            original["items"].append(2)
            release.set()
            assert await pending == {"items": [1]}
            for acceptor in acceptors:
                assert (await acceptor.get_state("key")).accepted_value == {"items": [1]}
        finally:
            release.set()
            await asyncio.gather(pending, return_exceptions=True)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "failure", ["lost_quorum", "partial_accept", "conflict", "minority_loss"]
    )
    async def test_uncertain_accept_does_not_reapply_update(self, failure):
        class FaultAcceptor(InMemoryAcceptor):
            first_accept = True

            async def accept(self, request):
                if not self.first_accept:
                    return await super().accept(request)
                self.first_accept = False
                if failure == "partial_accept" and self.node_id != "0":
                    raise ConnectionError("request lost before persistence")
                if failure == "conflict" and self.node_id == "2":
                    await super().prepare(
                        PrepareRequest(request.key, BallotNumber(2, "z"))
                    )
                    return await super().accept(request)
                response = await super().accept(request)
                if (
                    failure == "conflict"
                    or (failure == "lost_quorum" and self.node_id != "0")
                    or (failure == "minority_loss" and self.node_id == "2")
                ):
                    raise ConnectionError("reply lost after persistence")
                return response

        acceptors = [FaultAcceptor(str(i)) for i in range(3)]
        proposer = Proposer("p", acceptors)
        calls = 0

        def increment(value):
            nonlocal calls
            calls += 1
            return (value or 0) + 1

        if failure == "minority_loss":
            assert await proposer.change("counter", increment) == 1
        else:
            with pytest.raises(ProposerError) as error:
                await proposer.change("counter", increment)
            assert error.value.code == "INDETERMINATE"

        assert calls == 1
        recovered = await proposer.get("counter")
        if failure == "partial_accept":
            # A minority-only accept is not chosen: recovery may adopt it or
            # select an untouched quorum. It must never find a second increment.
            assert recovered in (None, 1)
        else:
            # A quorum persisted these writes even if replies were lost.
            assert recovered == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("failure", ["connection_error", "conflict"])
    async def test_prepare_failures_remain_retryable(self, failure):
        class FaultAcceptor(InMemoryAcceptor):
            first_prepare = True

            async def prepare(self, request):
                if self.first_prepare:
                    self.first_prepare = False
                    if failure == "connection_error":
                        raise ConnectionError("prepare unavailable")
                    await super().prepare(
                        PrepareRequest(request.key, BallotNumber(1, "z"))
                    )
                return await super().prepare(request)

        acceptors = [FaultAcceptor(str(i)) for i in range(3)]
        proposer = Proposer("p", acceptors)
        assert await proposer.change("counter", lambda value: (value or 0) + 1) == 1
        assert await proposer.get("counter") == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "expected,replacement,success",
        [
            ("wrong", "current", False),
            ("current", "current", True),
            ("wrong", "different", False),
        ],
    )
    async def test_compare_and_swap_reports_comparison(self, expected, replacement, success):
        proposer = Proposer(
            "p", [InMemoryAcceptor(str(i)) for i in range(3)]
        )
        await proposer.put("key", "current")
        assert await proposer.compare_and_swap("key", expected, replacement) == (
            success, "current"
        )
        assert await proposer.get("key") == "current"

    @pytest.mark.asyncio
    async def test_concurrent_compare_and_swap_has_one_winner(self):
        proposer = Proposer(
            "p", [InMemoryAcceptor(str(i)) for i in range(3)]
        )
        await proposer.put("key", "available")
        results = await asyncio.gather(
            proposer.compare_and_swap("key", "available", "claimed"),
            proposer.compare_and_swap("key", "available", "claimed"),
        )
        assert sorted(results) == [(False, "claimed"), (True, "claimed")]
        assert await proposer.get("key") == "claimed"

    @pytest.mark.asyncio
    async def test_takeover_advances_past_highest_conflicting_promise(self):
        acceptors = [InMemoryAcceptor(str(i)) for i in range(3)]
        incumbent = Proposer("incumbent", acceptors)
        for value in range(1, 21):
            await incumbent.put("counter", value)
        for acceptor, counter in zip(acceptors, [100, 1000, 500]):
            response = await acceptor.prepare(
                PrepareRequest("counter", BallotNumber(counter, "z"))
            )
            assert response.is_prepared

        newcomer = Proposer("new", acceptors, quorum_size=3)
        # One rejected prepare must suffice to learn all observed frontiers,
        # without losing the value accepted before those outstanding promises.
        assert await newcomer.change("counter", lambda value: value + 1, retries=2) == 21
        assert await newcomer.get("counter") == 21

    @pytest.mark.asyncio
    async def test_accept_conflict_advances_next_explicit_operation(self):
        class PreemptingAcceptor(InMemoryAcceptor):
            preempted = False

            async def accept(self, request):
                if request.ballot.node_id == "new" and not self.preempted:
                    self.preempted = True
                    await super().prepare(
                        PrepareRequest(request.key, BallotNumber(1000, "z"))
                    )
                return await super().accept(request)

        acceptors = [PreemptingAcceptor(str(i)) for i in range(3)]
        await Proposer("incumbent", acceptors).put("counter", 7)
        newcomer = Proposer("new", acceptors)
        with pytest.raises(ProposerError) as error:
            await newcomer.change("counter", lambda value: value + 1)
        assert error.value.code == "INDETERMINATE"
        # Accept conflicts still must not replay the failed update. They only
        # advance the ballot for an operation explicitly requested by the caller.
        assert await newcomer.change("counter", lambda value: value, retries=1) == 7

    @pytest.mark.asyncio
    async def test_successful_quorum_remembers_minority_conflicts(self):
        class OfflineAcceptor(InMemoryAcceptor):
            offline = False

            async def prepare(self, request):
                if self.offline:
                    raise ConnectionError("offline")
                return await super().prepare(request)

            async def accept(self, request):
                if self.offline:
                    raise ConnectionError("offline")
                return await super().accept(request)

        acceptors = [OfflineAcceptor(str(i)) for i in range(3)]
        await acceptors[2].prepare(PrepareRequest("counter", BallotNumber(1000, "z")))
        proposer = Proposer("p", acceptors)
        assert await proposer.change("counter", lambda _: 7, retries=1) == 7
        # The next quorum now needs the replica whose high promise was already
        # observed during the successful operation.
        acceptors[0].offline = True
        assert await proposer.change("counter", lambda value: value + 1, retries=1) == 8