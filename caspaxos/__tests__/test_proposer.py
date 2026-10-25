"""Tests for Proposer implementation."""

import pytest
import asyncio
from ..core import Proposer, ProposerError, InsufficientQuorumError
from ..core.ballot import BallotNumber
from ..storage.memory import InMemoryAcceptor
from ..core.messages import PrepareRequest
from ..core.proposer_enhanced import EnhancedProposer, ProposerError as EnhancedProposerError


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
    async def test_quorum_calculation(self):
        """Test quorum size calculation."""
        # 3 acceptors -> quorum = 2
        acceptors = [InMemoryAcceptor(f"a{i}") for i in range(3)]
        proposer = Proposer("p1", acceptors)
        assert proposer.quorum_size == 2
        
        # 5 acceptors -> quorum = 3
        acceptors = [InMemoryAcceptor(f"a{i}") for i in range(5)]
        proposer = Proposer("p1", acceptors)
        assert proposer.quorum_size == 3
        
        # Custom quorum size
        proposer = Proposer("p1", acceptors, quorum_size=4)
        assert proposer.quorum_size == 4
        
        # Invalid quorum size
        with pytest.raises(ValueError):
            Proposer("p1", acceptors, quorum_size=6)
    
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
        assert "Cannot update initial value" in str(exc_info.value)
        
        # Value should remain unchanged
        value = await proposer.change("key1", lambda x: x)
        assert value == "initial"
    
    @pytest.mark.asyncio
    async def test_ballot_advancement(self):
        """Test that ballot numbers advance properly."""
        acceptors = [InMemoryAcceptor(f"acceptor_{i}") for i in range(3)]
        proposer = Proposer("proposer1", acceptors)
        
        # Initial ballot
        initial_ballot = proposer.ballot.counter
        
        # First proposal
        await proposer.change("key1", lambda x: "value1")
        assert proposer.ballot.counter > initial_ballot
        
        # Second proposal (cached, no ballot increment)
        first_ballot = proposer.ballot.counter
        await proposer.change("key1", lambda x: "value2")
        # Ballot might not change if using cache
        
        # New key requires new ballot
        await proposer.change("key2", lambda x: "value3")
        assert proposer.ballot.counter > initial_ballot
    
    @pytest.mark.asyncio
    async def test_cache_invalidation(self):
        """Test that cache is properly managed."""
        acceptors = [InMemoryAcceptor(f"acceptor_{i}") for i in range(3)]
        proposer1 = Proposer("proposer1", acceptors)
        
        # First change caches the value
        await proposer1.change("key1", lambda x: "value1")
        assert "key1" in proposer1.cache
        
        # Second change on same key uses cache
        await proposer1.change("key1", lambda x: "value2")
        assert "key1" in proposer1.cache
        
        # Cache should contain the latest value
        cached_ballot, cached_value = proposer1.cache["key1"]
        assert cached_value == "value2"
    
    @pytest.mark.asyncio
    async def test_insufficient_quorum(self):
        """Test behavior with insufficient acceptors."""
        # Create 3 acceptors but require 4 for quorum (impossible)
        acceptors = [InMemoryAcceptor(f"acceptor_{i}") for i in range(3)]
        
        # Should raise ValueError when quorum > acceptors
        with pytest.raises(ValueError) as exc_info:
            proposer = Proposer("proposer1", acceptors, quorum_size=4)
        assert "Quorum size 4 > acceptors 3" in str(exc_info.value)

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
                assert error.value.code == "CommitError"
        finally:
            if not pending.done():
                pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)

        acceptors[1].fail = acceptors[2].fail = False
        assert await proposer.change("key", lambda _: "second") == "second"

        # A fresh proposer must recover the acknowledged value, not the
        # minority's earlier value from an ambiguously completed operation.
        reader = Proposer("z", acceptors)
        for _ in range(3):
            try:
                value = await reader.change("key", lambda value: value)
                break
            except ProposerError:
                continue
        else:
            pytest.fail("reader could not acquire a ballot")
        assert value == "second"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("proposer_type", [Proposer, EnhancedProposer])
    async def test_proposal_snapshot_precedes_accept_io(self, proposer_type):
        accepting = asyncio.Event()
        release = asyncio.Event()

        class DelayedAcceptor(InMemoryAcceptor):
            async def accept(self, request):
                accepting.set()
                await release.wait()
                return await super().accept(request)

        acceptors = [DelayedAcceptor(str(i)) for i in range(3)]
        proposer = proposer_type("p", acceptors)
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


class TestEnhancedProposer:
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
        proposer = EnhancedProposer("p", acceptors)
        if failure == "minority_loss":
            assert await proposer.change("counter", lambda value: (value or 0) + 1) == 1
        else:
            with pytest.raises(EnhancedProposerError) as error:
                await proposer.change("counter", lambda value: (value or 0) + 1)
            assert error.value.code == "INDETERMINATE"

        # Explicit recovery reads may adopt an uncertain write, but must never
        # discover two increments from this single client operation.
        assert await proposer.read("counter") == 1

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
        proposer = EnhancedProposer("p", acceptors)
        assert await proposer.change("counter", lambda value: (value or 0) + 1) == 1
        assert await proposer.read("counter") == 1

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
        proposer = EnhancedProposer(
            "p", [InMemoryAcceptor(str(i)) for i in range(3)]
        )
        await proposer.write("key", "current")
        assert await proposer.compare_and_swap("key", expected, replacement) == (
            success, "current"
        )
        assert await proposer.read("key") == "current"

    @pytest.mark.asyncio
    async def test_concurrent_compare_and_swap_has_one_winner(self):
        proposer = EnhancedProposer(
            "p", [InMemoryAcceptor(str(i)) for i in range(3)]
        )
        await proposer.write("key", "available")
        results = await asyncio.gather(
            proposer.compare_and_swap("key", "available", "claimed"),
            proposer.compare_and_swap("key", "available", "claimed"),
        )
        assert sorted(results) == [(False, "claimed"), (True, "claimed")]
        assert await proposer.read("key") == "claimed"