"""
Abstract storage backend interface for CASPaxos acceptors.

This module defines the storage abstraction that allows acceptors to use
different persistence mechanisms (in-memory, Redis, PostgreSQL, etc).
"""

from abc import ABC, abstractmethod
from typing import Callable, List, Optional, Tuple, TypeVar
from .acceptor import AcceptorState


Result = TypeVar("Result")


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
            A detached snapshot of AcceptorState, or None if not found.
            Mutating the returned ballots or value must not change stored state.
        """
        pass
    
    @abstractmethod
    async def set_state(self, key: str, state: AcceptorState) -> None:
        """
        Store or update the state for a given key.

        Store an independent snapshot, including ballots and nested values;
        subsequent caller mutations must not alter persisted state.
        
        Args:
            key: The key to store state for
            state: The AcceptorState to store
        """
        pass
    
    @abstractmethod
    async def update_state(
        self,
        key: str,
        transition: Callable[[Optional[AcceptorState]], Tuple[Optional[AcceptorState], Result]],
    ) -> Result:
        """
        Atomically read, transform, and persist state for one key.

        The synchronous transition receives a detached snapshot (or None) and
        returns (replacement, result). A None replacement means no write.
        Exceptions from the transition must leave stored state unchanged.
        Persist a detached snapshot before returning the result.

        Serialize the entire operation against other writes to this key,
        including calls from other acceptors sharing the same stored state.
        Persistent backends must enforce this with a transaction or equivalent
        conditional update, not separate get_state/set_state calls.
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