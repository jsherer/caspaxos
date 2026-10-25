"""Tests for acceptor implementations."""

import pytest
import asyncio
from ..core.ballot import BallotNumber
from ..core.messages import PrepareRequest, AcceptRequest
from ..storage.memory import InMemoryAcceptor
from ..storage.acceptor import AcceptorState
from ..acceptor import BaseAcceptor
from ..storage.backends.memory import InMemoryStorage


class TestInMemoryAcceptor:
    """Test suite for InMemoryAcceptor."""
    
    @pytest.mark.asyncio
    async def test_initial_prepare(self):
        """Test prepare on uninitialized key."""
        acceptor = InMemoryAcceptor("test_acceptor")
        
        # First prepare should succeed
        request = PrepareRequest("key1", BallotNumber(1, "node1"))
        response = await acceptor.prepare(request)
        
        assert response.is_prepared
        assert response.ballot.is_zero()
        assert response.value is None
        
        # State should be updated
        state = await acceptor.get_state("key1")
        assert state.promised_ballot == BallotNumber(1, "node1")
        assert state.accepted_ballot.is_zero()
        assert state.accepted_value is None
    
    @pytest.mark.asyncio
    async def test_prepare_with_lower_ballot(self):
        """Test prepare with lower ballot number."""
        acceptor = InMemoryAcceptor("test_acceptor")
        
        # First prepare with ballot 5
        request1 = PrepareRequest("key1", BallotNumber(5, "node1"))
        response1 = await acceptor.prepare(request1)
        assert response1.is_prepared
        
        # Second prepare with lower ballot should fail
        request2 = PrepareRequest("key1", BallotNumber(3, "node2"))
        response2 = await acceptor.prepare(request2)
        
        assert response2.is_conflict
        assert response2.ballot == BallotNumber(5, "node1")
    
    @pytest.mark.asyncio
    async def test_prepare_with_accepted_value(self):
        """Test prepare returns previously accepted value."""
        acceptor = InMemoryAcceptor("test_acceptor")
        
        # Prepare and accept a value
        prep_req = PrepareRequest("key1", BallotNumber(1, "node1"))
        prep_resp = await acceptor.prepare(prep_req)
        assert prep_resp.is_prepared
        
        accept_req = AcceptRequest(
            "key1",
            BallotNumber(1, "node1"),
            "value1",
            BallotNumber(2, "node1")
        )
        accept_resp = await acceptor.accept(accept_req)
        assert accept_resp.is_ok
        
        # New prepare should return accepted value
        prep_req2 = PrepareRequest("key1", BallotNumber(3, "node2"))
        prep_resp2 = await acceptor.prepare(prep_req2)
        
        assert prep_resp2.is_prepared
        assert prep_resp2.ballot == BallotNumber(1, "node1")
        assert prep_resp2.value == "value1"
    
    @pytest.mark.asyncio
    async def test_accept_success(self):
        """Test successful accept operation."""
        acceptor = InMemoryAcceptor("test_acceptor")
        
        # Prepare first
        prep_req = PrepareRequest("key1", BallotNumber(1, "node1"))
        await acceptor.prepare(prep_req)
        
        # Accept should succeed
        accept_req = AcceptRequest(
            "key1",
            BallotNumber(1, "node1"),
            "value1",
            BallotNumber(2, "node1")
        )
        response = await acceptor.accept(accept_req)
        
        assert response.is_ok
        
        # State should be updated
        state = await acceptor.get_state("key1")
        assert state.promised_ballot == BallotNumber(2, "node1")
        assert state.accepted_ballot == BallotNumber(1, "node1")
        assert state.accepted_value == "value1"
    
    @pytest.mark.asyncio
    async def test_accept_with_lower_ballot(self):
        """Test accept with lower ballot fails."""
        acceptor = InMemoryAcceptor("test_acceptor")
        
        # Prepare with ballot 5
        prep_req = PrepareRequest("key1", BallotNumber(5, "node1"))
        await acceptor.prepare(prep_req)
        
        # Accept with lower ballot should fail
        accept_req = AcceptRequest(
            "key1",
            BallotNumber(3, "node2"),
            "value1",
            BallotNumber(4, "node2")
        )
        response = await acceptor.accept(accept_req)
        
        assert response.is_conflict
        assert response.ballot == BallotNumber(5, "node1")
    
    @pytest.mark.asyncio
    async def test_accept_without_prepare(self):
        """Test accept can succeed without explicit prepare if ballot is high enough."""
        acceptor = InMemoryAcceptor("test_acceptor")
        
        # Accept with ballot 1 should succeed on new key
        accept_req = AcceptRequest(
            "key1",
            BallotNumber(1, "node1"),
            "value1",
            BallotNumber(2, "node1")
        )
        response = await acceptor.accept(accept_req)
        
        assert response.is_ok
        
        # State should be updated
        state = await acceptor.get_state("key1")
        assert state.accepted_value == "value1"
    
    @pytest.mark.asyncio
    async def test_concurrent_operations(self):
        """Test concurrent operations on different keys."""
        acceptor = InMemoryAcceptor("test_acceptor")
        
        async def prepare_and_accept(key, node_id, value):
            # Prepare
            prep_req = PrepareRequest(key, BallotNumber(1, node_id))
            prep_resp = await acceptor.prepare(prep_req)
            assert prep_resp.is_prepared
            
            # Accept
            accept_req = AcceptRequest(
                key,
                BallotNumber(1, node_id),
                value,
                BallotNumber(2, node_id)
            )
            accept_resp = await acceptor.accept(accept_req)
            assert accept_resp.is_ok
            
            return value
        
        # Run concurrent operations on different keys
        tasks = [
            prepare_and_accept(f"key{i}", f"node{i}", f"value{i}")
            for i in range(10)
        ]
        results = await asyncio.gather(*tasks)
        
        # Verify all succeeded
        assert results == [f"value{i}" for i in range(10)]
        
        # Verify states
        for i in range(10):
            state = await acceptor.get_state(f"key{i}")
            assert state.accepted_value == f"value{i}"
    
    @pytest.mark.asyncio
    async def test_clear_state(self):
        """Test clearing acceptor state."""
        acceptor = InMemoryAcceptor("test_acceptor")
        
        # Add some state
        prep_req = PrepareRequest("key1", BallotNumber(1, "node1"))
        await acceptor.prepare(prep_req)
        
        # Verify state exists
        state = await acceptor.get_state("key1")
        assert state is not None
        
        # Clear state
        await acceptor.clear()
        
        # State should be gone
        state = await acceptor.get_state("key1")
        assert state is None
        
        # New operations should work
        prep_req2 = PrepareRequest("key1", BallotNumber(1, "node1"))
        response = await acceptor.prepare(prep_req2)
        assert response.is_prepared


@pytest.mark.asyncio
@pytest.mark.parametrize("acceptor_type", [BaseAcceptor, InMemoryAcceptor])
@pytest.mark.parametrize("older_operation", ["prepare", "accept"])
async def test_atomic_transition_preserves_newer_accept(acceptor_type, older_operation):
    writing = asyncio.Event()
    release = asyncio.Event()

    class DelayedStorage(InMemoryStorage):
        """Model an async backend whose writes can be delayed before persistence."""

        def __init__(self):
            super().__init__()
            self.transactions = {}

        async def set_state(self, key, state):
            if key == "key" and state.promised_ballot.node_id == "old":
                writing.set()
                await release.wait()
            await super().set_state(key, state)

        async def update_state(self, key, transition):
            # A backend transaction must span the read, decision, and async write.
            lock = self.transactions.setdefault(key, asyncio.Lock())
            async with lock:
                state, result = transition(await self.get_state(key))
                if state is not None:
                    await self.set_state(key, state)
                return result

    storage = DelayedStorage()
    first = acceptor_type("replica")
    second = acceptor_type("replica")
    first.storage = second.storage = storage
    if older_operation == "prepare":
        request = first.prepare(PrepareRequest("key", BallotNumber(1, "old")))
    else:
        request = first.accept(
            AcceptRequest("key", BallotNumber(1, "old"), "old", BallotNumber(2, "old"))
        )
    pending = asyncio.create_task(request)
    competing = None

    async def commit_newer():
        prepared = await second.prepare(PrepareRequest("key", BallotNumber(3, "new")))
        assert prepared.is_prepared
        accepted = await second.accept(
            AcceptRequest("key", BallotNumber(3, "new"), "new", BallotNumber(4, "new"))
        )
        assert accepted.is_ok

    try:
        await asyncio.wait_for(writing.wait(), 1)
        competing = asyncio.create_task(commit_newer())
        # Run the competitor until it either completes or waits for the transaction.
        await asyncio.sleep(0)
        # A stalled key must not hold a backend-wide lock.
        other = await asyncio.wait_for(
            second.prepare(PrepareRequest("other", BallotNumber(1, "other"))), 1
        )
        assert other.is_prepared
        release.set()
        await asyncio.wait_for(asyncio.gather(pending, competing), 1)

        state = await storage.get_state("key")
        assert state.promised_ballot == BallotNumber(4, "new")
        assert state.accepted_ballot == BallotNumber(3, "new")
        assert state.accepted_value == "new"
    finally:
        release.set()
        tasks = [pending] + ([competing] if competing is not None else [])
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)