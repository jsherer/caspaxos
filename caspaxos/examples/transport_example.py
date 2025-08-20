#!/usr/bin/env python3
"""
Example demonstrating CASPaxos with transport abstraction.

This example shows how to use:
1. Local acceptors with direct communication
2. Remote acceptors via transport layer
3. Mixed configuration (both local and remote)
"""

import asyncio
import sys
import os

# Handle imports for both module and script execution
if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from caspaxos.acceptor import BaseAcceptor
from caspaxos.storage.backends.memory import InMemoryStorage
from caspaxos.transport.direct import DirectTransport
from caspaxos.transport.proxy import RemoteAcceptor
from caspaxos.core.proposer_enhanced import EnhancedProposer


async def local_acceptors_example():
    """Example using only local acceptors (traditional approach)."""
    print("\n=== Local Acceptors Example ===")
    
    # Create local acceptors with in-memory storage
    acceptors = [
        BaseAcceptor(f"acceptor_{i}", InMemoryStorage())
        for i in range(3)
    ]
    
    # Create proposer with local acceptors
    proposer = EnhancedProposer("proposer_1", acceptors=acceptors)
    
    # Perform some operations
    print("Writing initial value...")
    result = await proposer.write("config", {"version": 1})
    print(f"Written: {result}")
    
    print("Updating value...")
    result = await proposer.change(
        "config",
        lambda v: {"version": v.get("version", 0) + 1, "updated": True} if isinstance(v, dict) else {"version": 2, "updated": True}
    )
    print(f"Updated: {result}")
    
    print("Reading value...")
    result = await proposer.read("config")
    print(f"Read: {result}")


async def remote_acceptors_example():
    """Example using remote acceptors via transport."""
    print("\n=== Remote Acceptors via Transport Example ===")
    
    # Create transport layer
    transport = DirectTransport()
    
    # Create acceptors and register them with transport
    acceptors = []
    for i in range(3):
        acceptor = BaseAcceptor(f"remote_acceptor_{i}", InMemoryStorage())
        acceptors.append(acceptor)
        
        # Register acceptor handlers with transport
        await transport.register_acceptor(
            f"remote_acceptor_{i}",
            acceptor.prepare,
            acceptor.accept
        )
    
    # Create proposer that uses transport to reach acceptors
    proposer = EnhancedProposer(
        "proposer_2",
        transport=transport,
        remote_acceptor_ids=[f"remote_acceptor_{i}" for i in range(3)]
    )
    
    # Perform operations (same as before, but now via transport)
    print("Writing initial value via transport...")
    result = await proposer.write("settings", {"mode": "production"})
    print(f"Written: {result}")
    
    print("Updating value via transport...")
    result = await proposer.change(
        "settings",
        lambda v: {**v, "debug": False} if v else {"debug": False}
    )
    print(f"Updated: {result}")
    
    # Clean up
    await transport.close()


async def mixed_acceptors_example():
    """Example using both local and remote acceptors."""
    print("\n=== Mixed Local and Remote Acceptors Example ===")
    
    # Create transport
    transport = DirectTransport()
    
    # Create one local acceptor
    local_acceptor = BaseAcceptor("local_acceptor", InMemoryStorage())
    
    # Create two remote acceptors
    for i in range(2):
        acceptor = BaseAcceptor(f"remote_acceptor_{i}", InMemoryStorage())
        await transport.register_acceptor(
            f"remote_acceptor_{i}",
            acceptor.prepare,
            acceptor.accept
        )
    
    # Create proposer with mixed acceptors
    proposer = EnhancedProposer(
        "proposer_3",
        acceptors=[local_acceptor],  # Local acceptor
        transport=transport,
        remote_acceptor_ids=[f"remote_acceptor_{i}" for i in range(2)]  # Remote acceptors
    )
    
    print(f"Total acceptors: {len(proposer.acceptors)} (1 local, 2 remote)")
    print(f"Quorum size: {proposer.quorum_size}")
    
    # Perform distributed consensus
    print("Performing consensus with mixed acceptors...")
    result = await proposer.write("distributed_key", {"consensus": "achieved"})
    print(f"Consensus result: {result}")
    
    # Demonstrate compare-and-swap
    print("Testing compare-and-swap...")
    success, value = await proposer.compare_and_swap(
        "distributed_key",
        {"consensus": "achieved"},
        {"consensus": "achieved", "cas": "success"}
    )
    print(f"CAS success: {success}, Value: {value}")
    
    # Clean up
    await transport.close()


async def failure_handling_example():
    """Example showing how the system handles acceptor failures."""
    print("\n=== Failure Handling Example ===")
    
    # Create 5 acceptors (can tolerate 2 failures with quorum of 3)
    acceptors = [
        BaseAcceptor(f"acceptor_{i}", InMemoryStorage())
        for i in range(5)
    ]
    
    proposer = EnhancedProposer("proposer_4", acceptors=acceptors)
    print(f"Created {len(acceptors)} acceptors, quorum size: {proposer.quorum_size}")
    
    # Write initial value
    await proposer.write("resilient_key", {"status": "healthy"})
    print("Initial write successful")
    
    # Simulate 2 acceptors "failing" by removing them
    # (In real scenario, they might be network partitioned)
    failed_acceptors = acceptors[:2]
    proposer.acceptors = acceptors[2:]  # Only 3 acceptors remain
    print(f"Simulated failure of 2 acceptors, {len(proposer.acceptors)} remain")
    
    # System should still work with 3 acceptors (quorum of 3)
    try:
        result = await proposer.change(
            "resilient_key",
            lambda v: {**v, "after_failure": True} if v else {"after_failure": True}
        )
        print(f"Update after failure successful: {result}")
    except Exception as e:
        print(f"Update failed: {e}")
    
    # Restore one acceptor
    proposer.acceptors.append(failed_acceptors[0])
    print(f"Restored 1 acceptor, {len(proposer.acceptors)} available")
    
    # Should work even better now
    result = await proposer.read("resilient_key")
    print(f"Read after restoration: {result}")


async def main():
    """Run all examples."""
    print("CASPaxos Transport Architecture Examples")
    print("=" * 40)
    
    await local_acceptors_example()
    await remote_acceptors_example()
    await mixed_acceptors_example()
    await failure_handling_example()
    
    print("\n" + "=" * 40)
    print("All examples completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())