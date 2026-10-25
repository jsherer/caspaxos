"""Tests for main CASPaxos implementation."""

import pytest
import asyncio
from .. import CASPaxos, KeyValueStore, CASKeyValue
from ..core import ProposerError


class TestCASPaxos:
    """Test suite for CASPaxos main class."""
    
    @pytest.mark.asyncio
    async def test_basic_change(self):
        """Test basic change operation."""
        paxos = CASPaxos(num_acceptors=3)
        
        # Initial value is None
        value = await paxos.get("key1")
        assert value is None
        
        # Change to set value
        result = await paxos.change("key1", lambda x: "value1")
        assert result == "value1"
        
        # Get should return the value
        value = await paxos.get("key1")
        assert value == "value1"
    
    @pytest.mark.asyncio
    async def test_update_function(self):
        """Test update function is applied correctly."""
        paxos = CASPaxos(num_acceptors=3)
        
        # Initialize counter
        await paxos.put("counter", 0)
        
        # Increment counter
        result = await paxos.change("counter", lambda x: x + 1)
        assert result == 1
        
        result = await paxos.change("counter", lambda x: x + 1)
        assert result == 2
        
        # Multiply by 2
        result = await paxos.change("counter", lambda x: x * 2)
        assert result == 4
    
    @pytest.mark.asyncio
    async def test_concurrent_changes(self):
        """Test concurrent changes on different keys."""
        paxos = CASPaxos(num_acceptors=3)
        
        async def increment_key(key, times):
            for _ in range(times):
                await paxos.change(key, lambda x: (x or 0) + 1)
            return await paxos.get(key)
        
        # Run concurrent increments on different keys
        tasks = [
            increment_key(f"counter{i}", 10)
            for i in range(5)
        ]
        results = await asyncio.gather(*tasks)
        
        # Each counter should be 10
        assert all(r == 10 for r in results)
    
    @pytest.mark.asyncio
    async def test_quorum_with_failures(self):
        """Test that consensus works with minority failures."""
        # Create 5 acceptors (need 3 for quorum)
        paxos = CASPaxos(num_acceptors=5)
        
        # Should work normally
        await paxos.put("key1", "value1")
        assert await paxos.get("key1") == "value1"
        
        # TODO: Add test for acceptor failures when we have network simulation
    
    @pytest.mark.asyncio
    async def test_update_error_handling(self):
        """Test handling of errors in update function."""
        paxos = CASPaxos(num_acceptors=3)
        
        # Set initial value
        await paxos.put("key1", "initial")
        
        # Update function that raises error
        def bad_update(x):
            raise ValueError("Update failed")
        
        # Should raise ProposerError with update error
        with pytest.raises(ProposerError) as exc_info:
            await paxos.change("key1", bad_update)
        assert "UpdateError" in str(exc_info.value.code)
        
        # Value should remain unchanged
        assert await paxos.get("key1") == "initial"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "boundary", ["input", "put_result", "get_result", "callback_argument"]
    )
    async def test_value_snapshots_isolate_callers_from_consensus(self, boundary):
        paxos = CASPaxos()
        original = {"items": [1]}
        result = await paxos.put("key", original)
        if boundary == "input":
            exposed = original
        elif boundary == "put_result":
            exposed = result
        elif boundary == "get_result":
            exposed = await paxos.get("key")
        else:
            captured = []

            def capture(current):
                captured.append(current)
                return current

            await paxos.change("key", capture)
            exposed = captured[0]

        exposed["items"].append(2)
        for acceptor in paxos.acceptors:
            state = await acceptor.get_state("key")
            assert state.accepted_value == {"items": [1]}
        # A storage-only fix must not leave the proposer's fast path aliased.
        assert await paxos.get("key") == {"items": [1]}

    @pytest.mark.asyncio
    async def test_mutating_update_error_preserves_previous_value(self):
        paxos = CASPaxos()
        await paxos.put("key", {"items": [1]})

        def bad_update(current):
            current["items"].append(2)
            raise ValueError("update rejected")

        with pytest.raises(ProposerError) as error:
            await paxos.change("key", bad_update)
        assert error.value.code == "UpdateError"
        assert await paxos.get("key") == {"items": [1]}
        for acceptor in paxos.acceptors:
            assert (await acceptor.get_state("key")).accepted_value == {"items": [1]}


class TestKeyValueStore:
    """Test suite for KeyValueStore."""
    
    @pytest.mark.asyncio
    async def test_basic_operations(self):
        """Test basic get/put/delete operations."""
        kv = KeyValueStore()
        
        # Initial get returns None
        assert await kv.get("key1") is None
        assert not await kv.exists("key1")
        
        # Put value
        await kv.put("key1", "value1")
        assert await kv.get("key1") == "value1"
        assert await kv.exists("key1")
        
        # Update value
        await kv.put("key1", "value2")
        assert await kv.get("key1") == "value2"
        
        # Delete value
        deleted = await kv.delete("key1")
        assert deleted
        assert await kv.get("key1") is None
        assert not await kv.exists("key1")
        
        # Delete non-existent key
        deleted = await kv.delete("key2")
        assert not deleted
    
    @pytest.mark.asyncio
    async def test_complex_values(self):
        """Test storing complex values."""
        kv = KeyValueStore()
        
        # Store dict
        data = {"name": "Alice", "age": 30, "hobbies": ["reading", "coding"]}
        await kv.put("user1", data)
        retrieved = await kv.get("user1")
        assert retrieved == data
        
        # Store list
        items = [1, 2, 3, "four", {"five": 5}]
        await kv.put("list1", items)
        assert await kv.get("list1") == items

    @pytest.mark.asyncio
    async def test_repeated_delete_and_recreation(self):
        kv = KeyValueStore()
        # A stored None is a live entry, unlike a tombstone or a missing key.
        await kv.put("key", None)
        assert await kv.exists("key")
        assert await kv.delete("key") is True
        assert await kv.delete("key") is False
        assert await kv.get("key") is None
        assert not await kv.exists("key")

        await kv.put("key", "replacement")
        assert await kv.get("key") == "replacement"
        assert await kv.exists("key")
        assert await kv.delete("key") is True
        assert not await kv.exists("key")

    @pytest.mark.asyncio
    async def test_concurrent_deletes_report_one_removal(self):
        paxos = CASPaxos()
        first, second = KeyValueStore(paxos), KeyValueStore(paxos)
        await first.put("key", "value")
        results = await asyncio.gather(first.delete("key"), second.delete("key"))
        assert sorted(results) == [False, True]
        assert await second.get("key") is None
        assert not await first.exists("key")


class TestCASKeyValue:
    """Test suite for CASKeyValue with compare-and-swap."""
    
    @pytest.mark.asyncio
    async def test_versioned_operations(self):
        """Test versioned read/write operations."""
        cas = CASKeyValue()
        
        # Initial read returns version 0
        result = await cas.read("key1")
        assert result["version"] == 0
        assert result["value"] is None
        
        # Write with correct version
        result = await cas.write("key1", 0, "value1")
        assert result["version"] == 1
        assert result["value"] == "value1"
        
        # Read returns new version
        result = await cas.read("key1")
        assert result["version"] == 1
        assert result["value"] == "value1"
        
        # Write with wrong version fails
        with pytest.raises(Exception) as exc_info:
            await cas.write("key1", 0, "value2")
        assert "Version mismatch" in str(exc_info.value)
        
        # Write with correct version succeeds
        result = await cas.write("key1", 1, "value2")
        assert result["version"] == 2
        assert result["value"] == "value2"
    
    @pytest.mark.asyncio
    async def test_atomic_increment(self):
        """Test atomic increment operation."""
        cas = CASKeyValue()
        
        # Initial increment
        result = await cas.increment("counter")
        assert result == 1
        
        # Multiple increments
        for i in range(2, 11):
            result = await cas.increment("counter")
            assert result == i
        
        # Increment by delta
        result = await cas.increment("counter", 5)
        assert result == 15
        
        # Negative increment
        result = await cas.increment("counter", -3)
        assert result == 12
    
    @pytest.mark.asyncio
    async def test_concurrent_increments(self):
        """Test concurrent increment operations maintain consistency."""
        cas = CASKeyValue()
        
        async def increment_many(times):
            for _ in range(times):
                await cas.increment("shared_counter")
        
        # Run concurrent increments
        tasks = [increment_many(100) for _ in range(10)]
        await asyncio.gather(*tasks)
        
        # Should have exactly 1000 increments
        result = await cas.read("shared_counter")
        assert result["value"] == 1000
    
    @pytest.mark.asyncio
    async def test_optimistic_concurrency(self):
        """Test optimistic concurrency control with CAS."""
        cas = CASKeyValue()
        
        # Initialize value
        await cas.write("account", 0, 100)
        
        async def transfer(amount):
            """Attempt to transfer money with retry."""
            max_retries = 10
            for _ in range(max_retries):
                # Read current state
                state = await cas.read("account")
                current_balance = state["value"]
                version = state["version"]
                
                # Check sufficient funds
                if current_balance < amount:
                    return False
                
                # Try to update
                try:
                    new_balance = current_balance - amount
                    await cas.write("account", version, new_balance)
                    return True
                except:
                    # Version conflict, retry
                    await asyncio.sleep(0.001)
            
            return False
        
        # Run concurrent transfers
        tasks = [transfer(10) for _ in range(10)]
        results = await asyncio.gather(*tasks)
        
        # All transfers should succeed (100 - 10*10 = 0)
        assert all(results)
        
        # Final balance should be 0
        final = await cas.read("account")
        assert final["value"] == 0
        
        # One more transfer should fail
        assert not await transfer(10)