"""Tests for storage abstraction."""

import pytest
from ..storage.backend import StorageBackend
from ..storage.backends.memory import InMemoryStorage
from ..storage.acceptor import AcceptorState
from ..core.ballot import BallotNumber


class TestInMemoryStorage:
    """Test suite for InMemoryStorage backend."""
    
    @pytest.mark.asyncio
    async def test_initial_state(self):
        """Test that initial state is None."""
        storage = InMemoryStorage()
        state = await storage.get_state("test_key")
        assert state is None
    
    @pytest.mark.asyncio
    async def test_set_and_get_state(self):
        """Test setting and getting state."""
        storage = InMemoryStorage()
        
        # Create state
        state = AcceptorState(
            promised_ballot=BallotNumber(1, "node1"),
            accepted_ballot=BallotNumber(0, ""),
            accepted_value=None
        )
        
        # Set state
        await storage.set_state("key1", state)
        
        # Get state
        retrieved = await storage.get_state("key1")
        assert retrieved is not None
        assert retrieved.promised_ballot == BallotNumber(1, "node1")
        assert retrieved.accepted_ballot.is_zero()
        assert retrieved.accepted_value is None
    
    @pytest.mark.asyncio
    async def test_update_state(self):
        """Test updating existing state."""
        storage = InMemoryStorage()
        
        # Set initial state
        state1 = AcceptorState(
            promised_ballot=BallotNumber(1, "node1"),
            accepted_ballot=BallotNumber(0, ""),
            accepted_value=None
        )
        await storage.set_state("key1", state1)
        
        # Update state
        state2 = AcceptorState(
            promised_ballot=BallotNumber(2, "node1"),
            accepted_ballot=BallotNumber(1, "node1"),
            accepted_value="value1"
        )
        await storage.set_state("key1", state2)
        
        # Verify update
        retrieved = await storage.get_state("key1")
        assert retrieved.promised_ballot == BallotNumber(2, "node1")
        assert retrieved.accepted_ballot == BallotNumber(1, "node1")
        assert retrieved.accepted_value == "value1"
    
    @pytest.mark.asyncio
    async def test_delete_state(self):
        """Test deleting state."""
        storage = InMemoryStorage()
        
        # Set state
        state = AcceptorState.initial()
        await storage.set_state("key1", state)
        
        # Verify it exists
        assert await storage.get_state("key1") is not None
        
        # Delete state
        await storage.delete_state("key1")
        
        # Verify it's gone
        assert await storage.get_state("key1") is None
    
    @pytest.mark.asyncio
    async def test_list_keys(self):
        """Test listing all keys."""
        storage = InMemoryStorage()
        
        # Initially empty
        keys = await storage.list_keys()
        assert keys == []
        
        # Add some states
        await storage.set_state("key1", AcceptorState.initial())
        await storage.set_state("key2", AcceptorState.initial())
        await storage.set_state("key3", AcceptorState.initial())
        
        # List keys
        keys = await storage.list_keys()
        assert set(keys) == {"key1", "key2", "key3"}
        
        # Delete one
        await storage.delete_state("key2")
        
        # List again
        keys = await storage.list_keys()
        assert set(keys) == {"key1", "key3"}
    
    @pytest.mark.asyncio
    async def test_clear(self):
        """Test clearing all states."""
        storage = InMemoryStorage()
        
        # Add multiple states
        await storage.set_state("key1", AcceptorState.initial())
        await storage.set_state("key2", AcceptorState.initial())
        await storage.set_state("key3", AcceptorState.initial())
        
        # Verify they exist
        assert len(await storage.list_keys()) == 3
        
        # Clear all
        await storage.clear()
        
        # Verify all gone
        assert await storage.list_keys() == []
        assert await storage.get_state("key1") is None
        assert await storage.get_state("key2") is None
        assert await storage.get_state("key3") is None
    
    @pytest.mark.asyncio
    async def test_concurrent_access(self):
        """Test that concurrent access to same key is safe."""
        import asyncio
        
        storage = InMemoryStorage()
        
        async def increment_ballot(key: str, node_id: str):
            """Increment the promised ballot."""
            for i in range(10):
                state = await storage.get_state(key)
                if state is None:
                    state = AcceptorState.initial()
                
                # Increment ballot
                new_ballot = BallotNumber(
                    state.promised_ballot.counter + 1,
                    node_id
                )
                state.promised_ballot = new_ballot
                
                await storage.set_state(key, state)
                await asyncio.sleep(0.001)  # Small delay to encourage interleaving
        
        # Run concurrent updates
        await asyncio.gather(
            increment_ballot("shared_key", "node1"),
            increment_ballot("shared_key", "node2"),
            increment_ballot("shared_key", "node3")
        )
        
        # Final ballot should be 30 (3 tasks * 10 increments)
        final_state = await storage.get_state("shared_key")
        assert final_state.promised_ballot.counter == 30
    
    @pytest.mark.asyncio
    async def test_isolation_between_keys(self):
        """Test that operations on different keys don't interfere."""
        storage = InMemoryStorage()
        
        # Set different states for different keys
        state1 = AcceptorState(
            promised_ballot=BallotNumber(1, "node1"),
            accepted_ballot=BallotNumber(0, ""),
            accepted_value="value1"
        )
        
        state2 = AcceptorState(
            promised_ballot=BallotNumber(2, "node2"),
            accepted_ballot=BallotNumber(1, "node2"),
            accepted_value="value2"
        )
        
        await storage.set_state("key1", state1)
        await storage.set_state("key2", state2)
        
        # Verify isolation
        retrieved1 = await storage.get_state("key1")
        retrieved2 = await storage.get_state("key2")
        
        assert retrieved1.promised_ballot == BallotNumber(1, "node1")
        assert retrieved1.accepted_value == "value1"
        
        assert retrieved2.promised_ballot == BallotNumber(2, "node2")
        assert retrieved2.accepted_value == "value2"
        
        # Delete key1 shouldn't affect key2
        await storage.delete_state("key1")
        
        assert await storage.get_state("key1") is None
        assert await storage.get_state("key2") is not None