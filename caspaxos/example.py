#!/usr/bin/env python3
"""
Example usage of the CASPaxos implementation.

This script demonstrates various use cases for the CASPaxos consensus protocol,
including basic operations, key-value storage, and compare-and-swap operations.
"""

import asyncio
import random
import time
from typing import Any, Optional

# Import from the same package
from .caspaxos import CASPaxos, KeyValueStore, CASKeyValue


async def basic_example():
    """Demonstrate basic CASPaxos operations."""
    print("\n=== Basic CASPaxos Example ===")
    
    # Create a CASPaxos instance with 3 acceptors (tolerates 1 failure)
    paxos = CASPaxos(node_id="node1", num_acceptors=3)
    
    # Use the core change operation
    print("\n1. Using change() to set and update values:")
    
    # Set initial value
    result = await paxos.change("user:1", lambda x: {"name": "Alice", "age": 30})
    print(f"Initial value: {result}")
    
    # Update age
    result = await paxos.change("user:1", lambda user: {**user, "age": user["age"] + 1})
    print(f"After birthday: {result}")
    
    # Use convenience methods
    print("\n2. Using get() and put() convenience methods:")
    
    await paxos.put("user:2", {"name": "Bob", "age": 25})
    user2 = await paxos.get("user:2")
    print(f"User 2: {user2}")


async def key_value_store_example():
    """Demonstrate key-value store built on CASPaxos."""
    print("\n=== Key-Value Store Example ===")
    
    kv = KeyValueStore()
    
    print("\n1. Basic operations:")
    
    # Put values
    await kv.put("config:timeout", 30)
    await kv.put("config:retries", 3)
    await kv.put("config:debug", True)
    
    # Get values
    timeout = await kv.get("config:timeout")
    print(f"Timeout: {timeout}")
    
    # Check existence
    exists = await kv.exists("config:timeout")
    print(f"config:timeout exists: {exists}")
    
    # Delete value
    deleted = await kv.delete("config:debug")
    print(f"Deleted config:debug: {deleted}")
    
    print("\n2. Working with complex data:")
    
    # Store nested data structure
    user_data = {
        "id": "user123",
        "profile": {
            "name": "Charlie",
            "email": "charlie@example.com",
            "preferences": {
                "theme": "dark",
                "notifications": True
            }
        },
        "roles": ["user", "admin"]
    }
    
    await kv.put("user:charlie", user_data)
    retrieved = await kv.get("user:charlie")
    print(f"Retrieved user: {retrieved['profile']['name']}")
    print(f"User roles: {retrieved['roles']}")


async def cas_example():
    """Demonstrate compare-and-swap operations."""
    print("\n=== Compare-And-Swap Example ===")
    
    cas = CASKeyValue()
    
    print("\n1. Versioned updates:")
    
    # Initial write
    result = await cas.write("document:1", 0, {"title": "Draft", "content": "..."})
    print(f"Created document version {result['version']}")
    
    # Read current version
    doc = await cas.read("document:1")
    print(f"Current version: {doc['version']}, title: {doc['value']['title']}")
    
    # Update with correct version
    try:
        result = await cas.write(
            "document:1", 
            doc['version'],
            {**doc['value'], "title": "Final"}
        )
        print(f"Updated to version {result['version']}")
    except ValueError as e:
        print(f"Update failed: {e}")
    
    print("\n2. Atomic counter:")
    
    # Increment counter atomically
    for i in range(5):
        count = await cas.increment("page_views")
        print(f"Page views: {count}")
    
    # Increment by custom amount
    count = await cas.increment("page_views", 10)
    print(f"After bulk increment: {count}")


async def concurrent_example():
    """Demonstrate concurrent operations and consistency."""
    print("\n=== Concurrent Operations Example ===")
    
    paxos = CASPaxos(num_acceptors=3)
    
    print("\n1. Concurrent increments with consistency:")
    
    # Initialize counter
    await paxos.put("counter", 0)
    
    # Define concurrent increment task
    async def increment_counter(worker_id: int, increments: int):
        for _ in range(increments):
            await paxos.change("counter", lambda x: x + 1)
            # Small random delay to increase contention
            await asyncio.sleep(random.uniform(0, 0.01))
        return worker_id
    
    # Run concurrent workers
    workers = 5
    increments_per_worker = 20
    
    start_time = time.time()
    tasks = [
        increment_counter(i, increments_per_worker)
        for i in range(workers)
    ]
    await asyncio.gather(*tasks)
    elapsed = time.time() - start_time
    
    # Verify consistency
    final_count = await paxos.get("counter")
    expected = workers * increments_per_worker
    
    print(f"Workers: {workers}, Increments each: {increments_per_worker}")
    print(f"Expected: {expected}, Actual: {final_count}")
    print(f"Consistency: {'✓ PASS' if final_count == expected else '✗ FAIL'}")
    print(f"Time: {elapsed:.2f}s")
    
    print("\n2. Optimistic concurrency with retries:")
    
    cas = CASKeyValue()
    
    # Initialize shared resource
    await cas.write("balance", 0, 1000)
    
    async def transfer_with_retry(from_balance: str, amount: int, worker_id: int):
        """Transfer money with optimistic concurrency control."""
        max_retries = 10
        for attempt in range(max_retries):
            # Read current state
            state = await cas.read(from_balance)
            current = state["value"]
            version = state["version"]
            
            # Check sufficient funds
            if current < amount:
                return False, worker_id, "Insufficient funds"
            
            # Try to update
            try:
                new_balance = current - amount
                await cas.write(from_balance, version, new_balance)
                return True, worker_id, f"Transferred {amount}"
            except:
                # Version conflict, retry with backoff
                await asyncio.sleep(0.001 * (2 ** attempt))
        
        return False, worker_id, "Max retries exceeded"
    
    # Concurrent transfers
    transfer_tasks = [
        transfer_with_retry("balance", 100, i)
        for i in range(10)
    ]
    results = await asyncio.gather(*transfer_tasks)
    
    # Show results
    successful = sum(1 for r in results if r[0])
    final_balance = (await cas.read("balance"))["value"]
    
    print(f"Transfer attempts: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Final balance: {final_balance}")
    print(f"Expected: {1000 - (successful * 100)}")


async def distributed_lock_example():
    """Demonstrate using CASPaxos as a distributed lock."""
    print("\n=== Distributed Lock Example ===")
    
    paxos = CASPaxos(num_acceptors=3)
    
    class DistributedLock:
        """Simple distributed lock using CASPaxos."""
        
        def __init__(self, paxos: CASPaxos, lock_name: str):
            self.paxos = paxos
            self.lock_name = f"lock:{lock_name}"
        
        async def acquire(self, owner: str, timeout: float = 10.0) -> bool:
            """Try to acquire the lock."""
            
            def try_acquire(current):
                if current is None:
                    # Lock is free
                    return {"owner": owner, "acquired_at": time.time()}
                elif time.time() - current.get("acquired_at", 0) > timeout:
                    # Lock expired
                    return {"owner": owner, "acquired_at": time.time()}
                else:
                    # Lock is held
                    raise ValueError("Lock is held")
            
            try:
                await self.paxos.change(self.lock_name, try_acquire)
                return True
            except:
                return False
        
        async def release(self, owner: str) -> bool:
            """Release the lock if owned by caller."""
            
            def try_release(current):
                if current and current.get("owner") == owner:
                    return None
                else:
                    raise ValueError("Not the lock owner")
            
            try:
                await self.paxos.change(self.lock_name, try_release)
                return True
            except:
                return False
    
    # Create lock
    lock = DistributedLock(paxos, "resource1")
    
    # Simulate workers trying to acquire lock
    async def worker(worker_id: str):
        acquired = await lock.acquire(worker_id)
        if acquired:
            print(f"Worker {worker_id}: Acquired lock")
            # Do work
            await asyncio.sleep(0.1)
            released = await lock.release(worker_id)
            print(f"Worker {worker_id}: Released lock: {released}")
        else:
            print(f"Worker {worker_id}: Failed to acquire lock")
    
    # Run workers concurrently
    await asyncio.gather(*[worker(f"w{i}") for i in range(5)])


async def main():
    """Run all examples."""
    print("CASPaxos Python Implementation Examples")
    print("=" * 50)
    
    await basic_example()
    await key_value_store_example()
    await cas_example()
    await concurrent_example()
    await distributed_lock_example()
    
    print("\n" + "=" * 50)
    print("All examples completed successfully!")


if __name__ == "__main__":
    # Run as module: python -m caspaxos.example
    asyncio.run(main())