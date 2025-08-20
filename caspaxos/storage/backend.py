"""
Abstract storage backend interface for CASPaxos acceptors.

This module defines the storage abstraction that allows acceptors to use
different persistence mechanisms (in-memory, Redis, PostgreSQL, etc).
"""

from abc import ABC, abstractmethod
from typing import List, Optional
from .acceptor import AcceptorState


class StorageBackend(ABC):
    """
    Abstract base class for acceptor state storage.
    
    Implementations of this interface handle the persistence of acceptor
    state, allowing the protocol logic to remain independent of storage
    concerns.
    """
    
    @abstractmethod
    async def get_state(self, key: str) -> Optional[AcceptorState]:
        """
        Retrieve the state for a given key.
        
        Args:
            key: The key to retrieve state for
            
        Returns:
            The stored AcceptorState or None if not found
        """
        pass
    
    @abstractmethod
    async def set_state(self, key: str, state: AcceptorState) -> None:
        """
        Store or update the state for a given key.
        
        Args:
            key: The key to store state for
            state: The AcceptorState to store
        """
        pass
    
    @abstractmethod
    async def delete_state(self, key: str) -> None:
        """
        Delete the state for a given key.
        
        Args:
            key: The key to delete state for
        """
        pass
    
    @abstractmethod
    async def list_keys(self) -> List[str]:
        """
        List all keys that have stored state.
        
        Returns:
            List of all keys with state
        """
        pass
    
    @abstractmethod
    async def clear(self) -> None:
        """
        Clear all stored state.
        
        Useful for testing and cleanup.
        """
        pass
    
    async def exists(self, key: str) -> bool:
        """
        Check if a key has stored state.
        
        Args:
            key: The key to check
            
        Returns:
            True if the key has state, False otherwise
        """
        state = await self.get_state(key)
        return state is not None