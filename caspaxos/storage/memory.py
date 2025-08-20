"""
In-memory acceptor implementation for CASPaxos.

This implementation stores all state in memory and is suitable for testing
and development. For production use, consider a persistent backend like Redis.

This class is maintained for backward compatibility. New code should use
BaseAcceptor with InMemoryStorage backend.
"""

import asyncio
from typing import Dict, Optional
from .acceptor import Acceptor, AcceptorState
from .backends.memory import InMemoryStorage
from ..core.ballot import BallotNumber
from ..core.messages import PrepareRequest, PrepareResponse, AcceptRequest, AcceptResponse
from ..core.acceptor_logic import AcceptorLogic


class InMemoryAcceptor(Acceptor):
    """
    In-memory implementation of a CASPaxos acceptor.
    
    Thread-safe through the use of asyncio locks per key.
    
    Note: This class is maintained for backward compatibility.
    New code should use BaseAcceptor with InMemoryStorage.
    """
    
    def __init__(self, node_id: str = "acceptor"):
        """
        Initialize the in-memory acceptor.
        
        Args:
            node_id: Unique identifier for this acceptor node
        """
        self.node_id = node_id
        self.storage = InMemoryStorage()
        self.logic = AcceptorLogic()
    
    async def prepare(self, request: PrepareRequest) -> PrepareResponse:
        """
        Handle a prepare request using the new architecture.
        
        Delegates to storage and logic components.
        
        Args:
            request: The prepare request
            
        Returns:
            PrepareResponse with result
        """
        # Get current state from storage
        state = await self.storage.get_state(request.key)
        
        # Process using pure logic
        update, response = self.logic.process_prepare(state, request)
        
        # Apply state update if needed
        if update and update.should_update:
            await self.storage.set_state(request.key, update.state)
        
        return response
    
    async def accept(self, request: AcceptRequest) -> AcceptResponse:
        """
        Handle an accept request using the new architecture.
        
        Delegates to storage and logic components.
        
        Args:
            request: The accept request
            
        Returns:
            AcceptResponse with result
        """
        # Get current state from storage
        state = await self.storage.get_state(request.key)
        
        # Process using pure logic
        update, response = self.logic.process_accept(state, request)
        
        # Apply state update if needed
        if update and update.should_update:
            await self.storage.set_state(request.key, update.state)
        
        return response
    
    async def get_state(self, key: str) -> Optional[AcceptorState]:
        """
        Get the current state for a key.
        
        Args:
            key: The key to get state for
            
        Returns:
            Current state or None if key doesn't exist
        """
        return await self.storage.get_state(key)
    
    async def clear(self) -> None:
        """Clear all state (useful for testing)."""
        await self.storage.clear()
    
    def get_node_id(self) -> str:
        """Get the node ID of this acceptor."""
        return self.node_id