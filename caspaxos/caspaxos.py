"""
Main CASPaxos interface and example implementations.

This module provides the main CASPaxos class and example key-value store
implementations built on top of the consensus protocol.
"""

import asyncio
import json
from typing import Any, Callable, Dict, List, Optional, Tuple
from .core import Proposer
from .storage import InMemoryAcceptor, Acceptor
from .transport.base import Transport


class CASPaxos:
    """
    Main interface to the CASPaxos consensus protocol.
    
    This class provides a simple `change` method that ensures
    strongly consistent updates across a distributed system.
    """
    
    def __init__(
        self,
        node_id: str = "node1",
        acceptors: Optional[List[Acceptor]] = None,
        num_acceptors: int = 3,
        phase_timeout: float = 5.0,
        *,
        transport: Optional[Transport] = None,
        remote_acceptor_ids: Optional[List[str]] = None,
        quorum_size: Optional[int] = None
    ):
        """
        Initialize CASPaxos instance.
        
        Args:
            node_id: Unique identifier for this node
            acceptors: Local acceptor instances (optional)
            num_acceptors: Local defaults created only when no acceptors or remote IDs are supplied
            phase_timeout: Positive finite deadline in seconds for each protocol phase
            transport: Transport used for remote acceptors; caller owns its lifecycle
            remote_acceptor_ids: Remote acceptors to combine with local instances
            quorum_size: Quorum for the combined cluster (default: majority)
        """
        self.node_id = node_id
        
        if acceptors is None and not remote_acceptor_ids:
            acceptors = [
                InMemoryAcceptor(f"acceptor_{i}")
                for i in range(num_acceptors)
            ]
        self.proposer = Proposer(
            node_id,
            acceptors,
            quorum_size=quorum_size,
            phase_timeout=phase_timeout,
            transport=transport,
            remote_acceptor_ids=remote_acceptor_ids,
        )
        self.acceptors = self.proposer.acceptors
    
    async def change(
        self,
        key: str,
        update_fn: Callable[[Optional[Any]], Any],
        extra: Optional[Any] = None,
        *,
        retries: int = 10
    ) -> Any:
        """
        Execute a strongly consistent change operation.
        
        This is the core CASPaxos operation. It atomically:
        1. Reads the current value for the key
        2. Applies the update function to compute new value
        3. Writes the new value if consensus is reached
        
        Args:
            key: The key to operate on
            update_fn: Function that takes current value and returns new value
            extra: Optional data forwarded to protocol requests
            retries: Maximum prepare attempts; uncertain accepts are never retried
            
        Returns:
            The new value after applying update_fn
            
        Raises:
            ProposerError: If consensus cannot be reached
        """
        return await self.proposer.change(key, update_fn, extra, retries=retries)
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Read a value using identity update function.
        
        Args:
            key: The key to read
            
        Returns:
            Current value or None if key doesn't exist
        """
        return await self.proposer.get(key)
    
    async def put(self, key: str, value: Any) -> Any:
        """
        Write a value using constant update function.
        
        Args:
            key: The key to write
            value: The value to write
            
        Returns:
            The written value
        """
        return await self.proposer.put(key, value)

    async def compare_and_swap(
        self, key: str, expected: Any, new_value: Any
    ) -> Tuple[bool, Any]:
        """Return (matched, resulting_value) after a consensus-backed comparison."""
        return await self.proposer.compare_and_swap(key, expected, new_value)


class KeyValueStore:
    """
    Simple key-value store built on CASPaxos.
    
    Provides a dictionary-like interface with get/put/delete operations.
    """
    
    def __init__(self, paxos: Optional[CASPaxos] = None):
        """
        Initialize key-value store.
        
        Args:
            paxos: CASPaxos instance (creates default if None)
        """
        self.paxos = paxos or CASPaxos()
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Get a value by key.
        
        Args:
            key: The key to get
            
        Returns:
            The value or None if not found
        """
        result = await self.paxos.change(key, lambda x: x)
        return result.get("value") if isinstance(result, dict) else result
    
    async def put(self, key: str, value: Any) -> None:
        """
        Put a key-value pair.
        
        Args:
            key: The key to put
            value: The value to put
        """
        await self.paxos.change(key, lambda x: {"value": value})
    
    async def delete(self, key: str) -> bool:
        """
        Delete a key.
        
        Args:
            key: The key to delete
            
        Returns:
            True if a live entry was deleted; False if missing or already deleted.
            A stored None is a live entry.
        """
        deleted = False

        def delete_fn(current):
            nonlocal deleted
            deleted = current is not None and not (
                isinstance(current, dict) and current.get("deleted", False)
            )
            return {"value": None, "deleted": True} if deleted else current

        await self.paxos.change(key, delete_fn)
        return deleted
    
    async def exists(self, key: str) -> bool:
        """
        Check if a key exists.
        
        Args:
            key: The key to check
            
        Returns:
            True if key exists, False otherwise
        """
        result = await self.paxos.get(key)
        if result is None:
            return False
        if isinstance(result, dict):
            return not result.get("deleted", False)
        return True


class CASKeyValue:
    """
    Compare-and-swap key-value store built on CASPaxos.
    
    Provides versioned values with CAS operations for optimistic concurrency.
    """
    
    def __init__(self, paxos: Optional[CASPaxos] = None):
        """
        Initialize CAS key-value store.
        
        Args:
            paxos: CASPaxos instance (creates default if None)
        """
        self.paxos = paxos or CASPaxos()
    
    async def read(self, key: str) -> Dict[str, Any]:
        """
        Read a versioned value.
        
        Args:
            key: The key to read
            
        Returns:
            Dict with 'version' and 'value' fields
        """
        def read_fn(current):
            if current is None:
                return {"version": 0, "value": None}
            return current
        
        return await self.paxos.change(key, read_fn)
    
    async def write(self, key: str, version: int, value: Any) -> Dict[str, Any]:
        """
        Write a value with version check (compare-and-swap).
        
        Args:
            key: The key to write
            version: Expected version (must match current)
            value: The value to write
            
        Returns:
            Dict with new 'version' and 'value'
            
        Raises:
            ValueError: If version doesn't match current version
        """
        def cas_fn(current):
            if current is None:
                current = {"version": 0, "value": None}
            
            if current["version"] != version:
                raise ValueError(
                    f"Version mismatch: expected {version}, got {current['version']}"
                )
            
            return {"version": version + 1, "value": value}
        
        return await self.paxos.change(key, cas_fn)
    
    async def increment(self, key: str, delta: int = 1) -> int:
        """
        Atomically increment a counter.
        
        Args:
            key: The key to increment
            delta: Amount to increment by (default 1)
            
        Returns:
            New counter value
        """
        def inc_fn(current):
            if current is None:
                current = {"version": 0, "value": 0}
            
            new_value = (current.get("value", 0) or 0) + delta
            return {
                "version": current.get("version", 0) + 1,
                "value": new_value
            }
        
        result = await self.paxos.change(key, inc_fn)
        return result["value"]


# Synchronous wrapper for easier usage
class SyncCASPaxos:
    """
    Synchronous wrapper around async CASPaxos.
    
    Provides blocking interface for non-async code.
    """
    
    def __init__(self, *args, **kwargs):
        """Initialize with same arguments as CASPaxos."""
        self.paxos = CASPaxos(*args, **kwargs)
        self._loop = None
    
    def _get_loop(self):
        """Get or create event loop."""
        if self._loop is None:
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)
        return self._loop
    
    def change(self, key: str, update_fn: Callable[[Optional[Any]], Any]) -> Any:
        """Synchronous version of change."""
        loop = self._get_loop()
        return loop.run_until_complete(self.paxos.change(key, update_fn))
    
    def get(self, key: str) -> Optional[Any]:
        """Synchronous version of get."""
        loop = self._get_loop()
        return loop.run_until_complete(self.paxos.get(key))
    
    def put(self, key: str, value: Any) -> Any:
        """Synchronous version of put."""
        loop = self._get_loop()
        return loop.run_until_complete(self.paxos.put(key, value))