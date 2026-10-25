#!/usr/bin/env python3
"""Debug test to understand the issue."""

import asyncio
import sys
import os

# Handle imports for both module and script execution
if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from caspaxos.acceptor import BaseAcceptor
from caspaxos.storage.backends.memory import InMemoryStorage
from caspaxos.core.proposer import Proposer


class DebugAcceptor(BaseAcceptor):
    """Report the result of each prepare request exactly once."""

    async def prepare(self, request):
        print(f"\n{self.node_id} prepare request: {request}")
        try:
            response = await super().prepare(request)
        except Exception as error:
            print(f"{self.node_id} prepare error: {error}")
            raise
        if response.is_prepared:
            print(f"{self.node_id} promise: {response}")
        elif response.is_conflict:
            print(f"{self.node_id} conflict: {response}")
        else:
            print(f"{self.node_id} response: {response}")
        return response


async def test_basic():
    """Basic test."""
    acceptors = [
        DebugAcceptor(f"acceptor_{i}", InMemoryStorage())
        for i in range(3)
    ]
    
    proposer = Proposer("proposer_1", acceptors=acceptors)
    
    print(f"Number of acceptors: {len(proposer.acceptors)}")
    print(f"Quorum size: {proposer.quorum_size}")
    
    print("\nAttempting write through proposer...")
    result = await proposer.put("test_key", "test_value")
    print(f"Success! Written: {result}")


if __name__ == "__main__":
    asyncio.run(test_basic())