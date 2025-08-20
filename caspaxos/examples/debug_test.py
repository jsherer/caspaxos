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
from caspaxos.core.proposer_enhanced import EnhancedProposer
from caspaxos.core.ballot import BallotNumber
from caspaxos.core.messages import PrepareRequest


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
        BaseAcceptor(f"acceptor_{i}", InMemoryStorage())
        for i in range(3)
    ]
    
    proposer = EnhancedProposer("proposer_1", acceptors=acceptors)
    
    # Try a simple operation with debugging
    print("\nAttempting write through proposer...")
    try:
        # Monkey-patch to add debugging
        original_prepare_phase = proposer._prepare_phase
        
        async def debug_prepare_phase(key, ballot):
            request = PrepareRequest(key, ballot)
            print(f"\nPrepare phase - Request: {request}")
            
            # Send prepare requests to all acceptors concurrently
            tasks = [acceptor.prepare(request) for acceptor in proposer.acceptors]
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            
            print(f"Got {len(responses)} responses:")
            for i, resp in enumerate(responses):
                print(f"  Response {i}: type={type(resp)}, value={resp}")
                if hasattr(resp, '__dict__'):
                    print(f"    attributes: {resp.__dict__}")
            
            return await original_prepare_phase(key, ballot)
        
        proposer._prepare_phase = debug_prepare_phase
        
        result = await proposer.write("test_key", "test_value")
        print(f"Success! Written: {result}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(test_basic())