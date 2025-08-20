#!/usr/bin/env python3
"""Simple test of the new architecture."""

import asyncio
import sys
import os

# Handle imports for both module and script execution
if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from caspaxos.acceptor import BaseAcceptor
from caspaxos.storage.backends.memory import InMemoryStorage
from caspaxos.core.proposer_enhanced import EnhancedProposer


async def test_basic():
    """Basic test."""
    # Create local acceptors
    acceptors = [
        BaseAcceptor(f"acceptor_{i}", InMemoryStorage())
        for i in range(3)
    ]
    
    # Create proposer
    proposer = EnhancedProposer("proposer_1", acceptors=acceptors)
    
    try:
        # Simple write
        print("Writing value...")
        result = await proposer.write("test_key", "test_value")
        print(f"Success! Written: {result}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(test_basic())