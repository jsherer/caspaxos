"""
In-memory acceptor implementation for CASPaxos.

This implementation stores all state in memory and is suitable for testing
and development. For production use, consider a persistent backend like Redis.
"""

import asyncio
from typing import Dict, Optional
from .acceptor import Acceptor, AcceptorState
from ..core.ballot import BallotNumber
from ..core.messages import PrepareRequest, PrepareResponse, AcceptRequest, AcceptResponse


class InMemoryAcceptor(Acceptor):
    """
    In-memory implementation of a CASPaxos acceptor.
    
    Thread-safe through the use of asyncio locks per key.
    """
    
    def __init__(self, node_id: str = "acceptor"):
        """
        Initialize the in-memory acceptor.
        
        Args:
            node_id: Unique identifier for this acceptor node
        """
        self.node_id = node_id
        self.states: Dict[str, AcceptorState] = {}
        self.locks: Dict[str, asyncio.Lock] = {}
    
    def _get_lock(self, key: str) -> asyncio.Lock:
        """Get or create a lock for a key."""
        if key not in self.locks:
            self.locks[key] = asyncio.Lock()
        return self.locks[key]
    
    def _get_or_create_state(self, key: str) -> AcceptorState:
        """Get existing state or create initial state for a key."""
        if key not in self.states:
            self.states[key] = AcceptorState.initial()
        return self.states[key]
    
    async def prepare(self, request: PrepareRequest) -> PrepareResponse:
        """
        Handle a prepare request.
        
        Algorithm:
        1. If ballot > promised_ballot and ballot > accepted_ballot:
           - Update promised_ballot to ballot
           - Return success with accepted value (if any)
        2. Otherwise return conflict with highest ballot seen
        
        Args:
            request: The prepare request
            
        Returns:
            PrepareResponse with result
        """
        async with self._get_lock(request.key):
            state = self._get_or_create_state(request.key)
            
            # Check if we can promise this ballot
            if (request.ballot > state.promised_ballot and 
                request.ballot > state.accepted_ballot):
                # Update promise
                state.promised_ballot = request.ballot
                
                # Return success with accepted value (if any)
                return PrepareResponse(
                    is_prepared=True,
                    ballot=state.accepted_ballot,
                    value=state.accepted_value
                )
            else:
                # Return conflict with highest ballot we've seen
                highest_ballot = max(state.promised_ballot, state.accepted_ballot)
                return PrepareResponse(
                    is_conflict=True,
                    ballot=highest_ballot
                )
    
    async def accept(self, request: AcceptRequest) -> AcceptResponse:
        """
        Handle an accept request.
        
        Algorithm:
        1. If ballot >= promised_ballot and ballot > accepted_ballot:
           - Update promised_ballot to promise
           - Update accepted_ballot to ballot
           - Update accepted_value to value
           - Return success
        2. Otherwise return conflict with highest ballot seen
        
        Args:
            request: The accept request
            
        Returns:
            AcceptResponse with result
        """
        async with self._get_lock(request.key):
            state = self._get_or_create_state(request.key)
            
            # Check if we can accept this ballot
            if (request.ballot >= state.promised_ballot and 
                request.ballot > state.accepted_ballot):
                # Accept the value
                state.promised_ballot = request.promise
                state.accepted_ballot = request.ballot
                state.accepted_value = request.value
                
                return AcceptResponse(is_ok=True)
            else:
                # Return conflict with highest ballot we've seen
                highest_ballot = max(state.promised_ballot, state.accepted_ballot)
                return AcceptResponse(
                    is_conflict=True,
                    ballot=highest_ballot
                )
    
    async def get_state(self, key: str) -> Optional[AcceptorState]:
        """
        Get the current state for a key.
        
        Args:
            key: The key to get state for
            
        Returns:
            Current state or None if key doesn't exist
        """
        async with self._get_lock(key):
            return self.states.get(key)
    
    async def clear(self) -> None:
        """Clear all state (useful for testing)."""
        self.states.clear()
        self.locks.clear()