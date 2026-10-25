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
from caspaxos.core.ballot import BallotNumber
from caspaxos.core.messages import PrepareRequest


class DebugAcceptor(BaseAcceptor):
    """Log each actual prepare request without sending a second one."""

    async def prepare(self, request):
        print(f"\n{self.node_id} prepare request: {request}")
        response = await super().prepare(request)
        print(f"{self.node_id} response: {response}")
        print(f"Response type: {type(response)}")
        return response


async def test_basic():
    """Basic test."""
    # Create one acceptor
    acceptor = BaseAcceptor("acceptor_1", InMemoryStorage())
    
    # Test prepare directly
    request = PrepareRequest("test_key", BallotNumber(1, "proposer"))
    response = await acceptor.prepare(request)
    
    print(f"Request: {request}")
    print(f"Response: {response}")
    print(f"Response type: {type(response)}")
    print(f"Response is_prepared: {getattr(response, 'is_prepared', 'NO ATTRIBUTE')}")
    
    # Now test with proposer
    acceptors = [
        DebugAcceptor(f"acceptor_{i}", InMemoryStorage())
        for i in range(3)
    ]
    
    proposer = Proposer("proposer_1", acceptors=acceptors)

    print("\nAttempting write through proposer...")
    result = await proposer.put("test_key", "test_value")
    print(f"Success! Written: {result}")


if __name__ == "__main__":
    asyncio.run(test_basic())