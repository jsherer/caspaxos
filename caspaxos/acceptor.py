"""
Base acceptor implementation combining protocol logic and storage.

This module provides the BaseAcceptor class that combines the pure
protocol logic with a pluggable storage backend.
"""

from typing import Optional, Tuple
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
        def transition(
            current_state: Optional[AcceptorState],
        ) -> Tuple[Optional[AcceptorState], PrepareResponse]:
            update, response = self.logic.process_prepare(current_state, request)
            return (update.state if update.should_update else None), response

        return await self.storage.update_state(request.key, transition)
    
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
        def transition(
            current_state: Optional[AcceptorState],
        ) -> Tuple[Optional[AcceptorState], AcceptResponse]:
            update, response = self.logic.process_accept(current_state, request)
            return (update.state if update.should_update else None), response

        return await self.storage.update_state(request.key, transition)
    
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