"""
In-memory storage backend for CASPaxos acceptors.

This implementation stores all state in memory using Python dictionaries.
It's suitable for testing, development, and single-node deployments.
"""

import asyncio
from copy import deepcopy
from typing import Dict, List, Optional
from ..backend import StorageBackend
from ..acceptor import AcceptorState


class InMemoryStorage(StorageBackend):
    """
    In-memory implementation of the storage backend.
    
    Uses dictionaries to store state with per-key locking for thread safety.
    All data is lost when the process terminates.
    """
    
    def __init__(self):
        """Initialize empty in-memory storage."""
        self.states: Dict[str, AcceptorState] = {}
        self.locks: Dict[str, asyncio.Lock] = {}
        self.global_lock = asyncio.Lock()
    
    def _get_lock(self, key: str) -> asyncio.Lock:
        """Get or create a lock for a specific key."""
        if key not in self.locks:
            self.locks[key] = asyncio.Lock()
        return self.locks[key]
    
    async def get_state(self, key: str) -> Optional[AcceptorState]:
        """
        Retrieve the state for a given key.
        
        Args:
            key: The key to retrieve state for
            
        Returns:
            A detached snapshot of AcceptorState, or None if not found
        """
        async with self._get_lock(key):
            return deepcopy(self.states.get(key))
    
    async def set_state(self, key: str, state: AcceptorState) -> None:
        """
        Store an independent snapshot of the state for a given key.
        
        Args:
            key: The key to store state for
            state: The AcceptorState to store
        """
        async with self._get_lock(key):
            self.states[key] = deepcopy(state)
    
    async def delete_state(self, key: str) -> None:
        """
        Delete the state for a given key.
        
        Args:
            key: The key to delete state for
        """
        async with self._get_lock(key):
            self.states.pop(key, None)
            # Also clean up the lock if no longer needed
            if key in self.locks and key not in self.states:
                del self.locks[key]
    
    async def list_keys(self) -> List[str]:
        """
        List all keys that have stored state.
        
        Returns:
            List of all keys with state
        """
        async with self.global_lock:
            return list(self.states.keys())
    
    async def clear(self) -> None:
        """
        Clear all stored state and locks.
        """
        async with self.global_lock:
            self.states.clear()
            self.locks.clear()