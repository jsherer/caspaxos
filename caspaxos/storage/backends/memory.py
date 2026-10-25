"""
In-memory storage backend for CASPaxos acceptors.

This implementation stores all state in memory using Python dictionaries.
It's suitable for testing, development, and single-node deployments.
"""

import asyncio
from copy import deepcopy
from typing import Callable, Dict, List, Optional, Tuple
from ..backend import Result, StorageBackend
from ..acceptor import AcceptorState


class InMemoryStorage(StorageBackend):
    """
    In-memory implementation of the storage backend.
    
    Uses dictionaries with per-key locking for concurrent asyncio operations.
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
    
    async def update_state(
        self,
        key: str,
        transition: Callable[[Optional[AcceptorState]], Tuple[Optional[AcceptorState], Result]],
    ) -> Result:
        """Apply a transition while holding the key's lock through persistence."""
        async with self._get_lock(key):
            state, result = transition(deepcopy(self.states.get(key)))
            if state is not None:
                self.states[key] = deepcopy(state)
            return result

    async def delete_state(self, key: str) -> None:
        """
        Delete the state for a given key.
        
        Args:
            key: The key to delete state for
        """
        async with self._get_lock(key):
            self.states.pop(key, None)
    
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
        Clear all stored state, retaining locks for any queued operations.
        """
        async with self.global_lock:
            self.states.clear()