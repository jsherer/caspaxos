#!/usr/bin/env python3
"""Debug test to understand the issue in accept phase."""

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
from caspaxos.core.messages import PrepareRequest, AcceptRequest


async def test_basic():
    """Basic test."""
    acceptors = [
        BaseAcceptor(f"acceptor_{i}", InMemoryStorage())
        for i in range(3)
    ]
    
    proposer = EnhancedProposer("proposer_1", acceptors=acceptors)
    
    print(f"Number of acceptors: {len(proposer.acceptors)}")
    print(f"Quorum size: {proposer.quorum_size}")
    
    # Try a simple operation with debugging
    print("\nAttempting write through proposer...")
    try:
        # Monkey-patch to add debugging
        original_accept_phase = proposer._accept_phase
        
        async def debug_accept_phase(key, ballot, value):
            # For CASPaxos, promise equals ballot for accept
            request = AcceptRequest(key, ballot, ballot, value)
            print(f"\nAccept phase - Request: {request}")
            
            # Send accept requests to all acceptors concurrently
            tasks = [acceptor.accept(request) for acceptor in proposer.acceptors]
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            
            print(f"Got {len(responses)} responses")
            
            # Count successful accepts
            accepts = []
            conflicts = []
            errors = []
            
            for resp in responses:
                if isinstance(resp, Exception):
                    errors.append(resp)
                    print(f"  Error: {resp}")
                elif isinstance(resp, AcceptResponse):
                    if resp.is_ok:
                        accepts.append(resp)
                        print(f"  Accept OK: {resp}")
                    elif resp.is_conflict:
                        conflicts.append(resp)
                        print(f"  Conflict: {resp}")
                    else:
                        print(f"  Unknown response: {resp}")
            
            print(f"Summary: {len(accepts)} accepts, {len(conflicts)} conflicts, {len(errors)} errors")
            print(f"Quorum check: {len(accepts)} >= {proposer.quorum_size} = {len(accepts) >= proposer.quorum_size}")
            
            if len(accepts) < proposer.quorum_size:
                print(f"NOT ENOUGH ACCEPTS! {len(accepts)} < {proposer.quorum_size}")
                if conflicts:
                    highest_conflict = max(conflicts, key=lambda r: r.ballot)
                    print(f"Highest conflict ballot: {highest_conflict.ballot}")
            
            return await original_accept_phase(key, ballot, value)
        
        proposer._accept_phase = debug_accept_phase
        
        result = await proposer.write("test_key", "test_value")
        print(f"Success! Written: {result}")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    from caspaxos.core.messages import AcceptResponse
    asyncio.run(test_basic())