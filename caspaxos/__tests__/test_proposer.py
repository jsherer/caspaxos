"""Tests for Proposer implementation."""

import pytest
import asyncio
from ..core import Proposer, ProposerError, InsufficientQuorumError
from ..core.ballot import BallotNumber
from ..storage.memory import InMemoryAcceptor


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