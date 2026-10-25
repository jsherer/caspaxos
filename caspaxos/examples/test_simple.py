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
from caspaxos.core.proposer import Proposer


async def test_basic():
    """Basic test."""
    # Create local acceptors
    acceptors = [
        BaseAcceptor(f"acceptor_{i}", InMemoryStorage())
        for i in range(3)
    ]
    
    # Create proposer
    proposer = Proposer("proposer_1", acceptors=acceptors)

    print("Writing value...")
    result = await proposer.put("test_key", "test_value")
    print(f"Success! Written: {result}")


if __name__ == "__main__":
    asyncio.run(test_basic())