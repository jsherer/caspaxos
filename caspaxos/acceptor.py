"""
Base acceptor implementation combining protocol logic and storage.

This module provides the BaseAcceptor class that combines the pure
protocol logic with a pluggable storage backend.
"""

from typing import Optional
from .core.acceptor_logic import AcceptorLogic
from .core.messages import PrepareRequest, PrepareResponse, AcceptRequest, AcceptResponse
from .storage.backend import StorageBackend
from .storage.backends import InMemoryStorage
from .storage.acceptor import Acceptor, AcceptorState


class BaseAcceptor(Acceptor):
    """
    Base acceptor implementation with pluggable storage.
    
    This acceptor combines the pure protocol logic with a storage backend,
    allowing different persistence mechanisms while keeping the protocol
    logic independent.
    """
    
    def __init__(
        self,
        node_id: str = "acceptor",
        storage: Optional[StorageBackend] = None
    ):
        """
        Initialize the acceptor.
        
        Args:
            node_id: Unique identifier for this acceptor
            storage: Storage backend to use (defaults to in-memory)
        """
        self.node_id = node_id
        self.storage = storage or InMemoryStorage()
        self.logic = AcceptorLogic()
    
    async def prepare(self, request: PrepareRequest) -> PrepareResponse:
        """
        Handle a prepare request.
        
        Retrieves current state from storage, applies protocol logic,
        updates storage if needed, and returns the response.
        
        Args:
            request: The prepare request
            
        Returns:
            PrepareResponse with result
        """
        # Get current state from storage
        current_state = await self.storage.get_state(request.key)
        
        # Apply protocol logic
        state_update, response = self.logic.process_prepare(current_state, request)
        
        # Update storage if needed
        if state_update.should_update:
            await self.storage.set_state(request.key, state_update.state)
        
        return response
    
    async def accept(self, request: AcceptRequest) -> AcceptResponse:
        """
        Handle an accept request.
        
        Retrieves current state from storage, applies protocol logic,
        updates storage if needed, and returns the response.
        
        Args:
            request: The accept request
            
        Returns:
            AcceptResponse with result
        """
        # Get current state from storage
        current_state = await self.storage.get_state(request.key)
        
        # Apply protocol logic
        state_update, response = self.logic.process_accept(current_state, request)
        
        # Update storage if needed
        if state_update.should_update:
            await self.storage.set_state(request.key, state_update.state)
        
        return response
    
    async def get_state(self, key: str) -> Optional[AcceptorState]:
        """
        Get the current state for a key.
        
        Args:
            key: The key to get state for
            
        Returns:
            Current acceptor state or None if key doesn't exist
        """
        return await self.storage.get_state(key)
    
    async def clear(self) -> None:
        """
        Clear all stored state.
        
        Useful for testing and cleanup.
        """
        await self.storage.clear()
    
    def get_node_id(self) -> str:
        """Get the node ID of this acceptor."""
        return self.node_id