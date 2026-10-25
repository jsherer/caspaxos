"""
Enhanced proposer implementation with transport support.

This proposer can work with both local acceptors and remote acceptors
accessed through a transport layer.
"""

import asyncio
from copy import deepcopy
from math import isfinite
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from .ballot import BallotNumber
from .messages import PrepareRequest, PrepareResponse, AcceptRequest, AcceptResponse
from .quorum import collect_responses
from ..storage.acceptor import Acceptor
from ..transport.base import Transport
from ..transport.proxy import RemoteAcceptor


class ProposerError(Exception):
    """Base exception for proposer errors."""
    
    def __init__(self, code: str, message: str = ""):
        super().__init__(message or code)
        self.code = code


class InsufficientQuorumError(Exception):
    """Raised when insufficient acceptors respond successfully."""
    
    def __init__(self, responses: List[Any]):
        super().__init__(f"Insufficient quorum: {len(responses)} responses")
        self.responses = responses


class EnhancedProposer:
    """
    Enhanced CASPaxos proposer with transport support.
    
    This proposer can work with:
    - Local acceptors (direct object references)
    - Remote acceptors via transport layer
    - Mixed configurations (some local, some remote)
    """
    
    def __init__(
        self,
        node_id: str,
        acceptors: Optional[List[Acceptor]] = None,
        transport: Optional[Transport] = None,
        remote_acceptor_ids: Optional[List[str]] = None,
        quorum_size: Optional[int] = None,
        phase_timeout: float = 5.0
    ):
        """
        Initialize the enhanced proposer.
        
        Args:
            node_id: Unique identifier for this proposer
            acceptors: List of local acceptor instances (optional)
            transport: Transport layer for remote acceptors (optional)
            remote_acceptor_ids: IDs of remote acceptors (optional)
            quorum_size: Size of quorum needed (default: majority)
            phase_timeout: Positive finite deadline in seconds for each protocol phase
        """
        self.node_id = node_id
        self.transport = transport
        
        # Build acceptor list
        self.acceptors: List[Acceptor] = []
        
        # Add local acceptors
        if acceptors:
            self.acceptors.extend(acceptors)
        
        # Add remote acceptors via transport
        if transport and remote_acceptor_ids:
            for acceptor_id in remote_acceptor_ids:
                proxy = RemoteAcceptor(acceptor_id, transport)
                self.acceptors.append(proxy)
        
        if not self.acceptors:
            raise ValueError("No acceptors provided (neither local nor remote)")
        
        # Calculate quorum size
        self.quorum_size = quorum_size or (len(self.acceptors) // 2 + 1)
        
        if self.quorum_size > len(self.acceptors):
            raise ValueError(f"Quorum size {self.quorum_size} > acceptors {len(self.acceptors)}")

        if not isfinite(phase_timeout) or phase_timeout <= 0:
            raise ValueError("phase_timeout must be positive and finite")
        self.phase_timeout = phase_timeout
        
        # Track ballot numbers per key
        self.ballot_counters: Dict[str, int] = {}
        
        # Locks for each key to prevent concurrent operations
        self.key_locks: Dict[str, asyncio.Lock] = {}
        
        # Cache for known values (optimization)
        self.value_cache: Dict[str, Tuple[BallotNumber, Any]] = {}
    
    def _get_lock(self, key: str) -> asyncio.Lock:
        """Get or create a lock for a key."""
        if key not in self.key_locks:
            self.key_locks[key] = asyncio.Lock()
        return self.key_locks[key]
    
    def _next_ballot(self, key: str) -> BallotNumber:
        """Generate the next ballot number for a key."""
        counter = self.ballot_counters.get(key, 0) + 1
        self.ballot_counters[key] = counter
        return BallotNumber(counter, self.node_id)
    
    async def _prepare_phase(
        self,
        key: str,
        ballot: BallotNumber
    ) -> Tuple[BallotNumber, Optional[Any]]:
        """
        Execute prepare phase.
        
        Returns:
            Tuple of (highest accepted ballot, accepted value)
            
        Raises:
            InsufficientQuorumError: If quorum not reached
        """
        request = PrepareRequest(key, ballot)
        
        # Send prepare requests to all acceptors concurrently
        tasks = [acceptor.prepare(request) for acceptor in self.acceptors]
        responses = await collect_responses(
            tasks,
            self.quorum_size,
            lambda response: isinstance(response, PrepareResponse) and response.is_prepared,
            self.phase_timeout,
        )
        
        # Count successful promises
        promises = []
        conflicts = []
        errors = []
        
        for resp in responses:
            if isinstance(resp, Exception):
                errors.append(resp)
            elif isinstance(resp, PrepareResponse):
                if resp.is_prepared:
                    promises.append(resp)
                elif resp.is_conflict:
                    conflicts.append(resp)
        
        # Check if we have quorum
        if len(promises) < self.quorum_size:
            # If we have conflicts, we might need to retry with higher ballot
            if conflicts:
                highest_conflict = max(conflicts, key=lambda r: r.ballot)
                raise ProposerError(
                    "CONFLICT",
                    f"Prepare failed: conflict with ballot {highest_conflict.ballot}"
                )
            raise InsufficientQuorumError(responses)
        
        # Find the highest accepted value
        highest_ballot = BallotNumber.zero()
        accepted_value = None
        
        for promise in promises:
            if promise.ballot and promise.ballot > highest_ballot:
                highest_ballot = promise.ballot
                accepted_value = promise.value
        
        return highest_ballot, accepted_value
    
    async def _accept_phase(
        self,
        key: str,
        ballot: BallotNumber,
        value: Any
    ) -> None:
        """
        Execute accept phase.
        
        Raises:
            InsufficientQuorumError: If quorum not reached
        """
        # For CASPaxos, promise equals ballot for accept
        request = AcceptRequest(key, ballot, value, ballot)
        
        # Send accept requests to all acceptors concurrently
        tasks = [acceptor.accept(request) for acceptor in self.acceptors]
        responses = await collect_responses(
            tasks,
            self.quorum_size,
            lambda response: isinstance(response, AcceptResponse) and response.is_ok,
            self.phase_timeout,
        )
        
        # Count successful accepts
        accepts = []
        conflicts = []
        errors = []
        
        for resp in responses:
            if isinstance(resp, Exception):
                errors.append(resp)
            elif isinstance(resp, AcceptResponse):
                if resp.is_ok:
                    accepts.append(resp)
                elif resp.is_conflict:
                    conflicts.append(resp)
        
        # Check if we have quorum
        if len(accepts) < self.quorum_size:
            # A conflict does not rule out acceptance by other replicas.
            if conflicts:
                highest_conflict = max(conflicts, key=lambda r: r.ballot)
                raise ProposerError(
                    "CONFLICT",
                    f"Accept failed: conflict with ballot {highest_conflict.ballot}"
                )
            raise InsufficientQuorumError(responses)
    
    async def change(
        self,
        key: str,
        update_fn: Callable[[Optional[Any]], Any],
        retries: int = 10
    ) -> Any:
        """
        Perform a CAS operation using the provided update function.
        
        Args:
            key: The key to update
            update_fn: Function that takes current value and returns new value
            retries: Maximum number of prepare attempts
            
        Returns:
            The new value after successful consensus
            
        Raises:
            ProposerError: If preparation fails, or with code INDETERMINATE
                when an accept quorum cannot be confirmed. The update may
                already have been applied and is not automatically retried.
        """
        async with self._get_lock(key):
            last_error = None
            
            for attempt in range(retries):
                try:
                    # Generate ballot for this attempt
                    ballot = self._next_ballot(key)
                    
                    # Phase 1: Prepare
                    _, current_value = await self._prepare_phase(key, ballot)
                    
                except ProposerError as e:
                    last_error = e
                    if e.code == "CONFLICT":
                        # Retry with higher ballot
                        await asyncio.sleep(0.01 * (2 ** attempt))  # Exponential backoff
                        continue
                    raise
                except InsufficientQuorumError as e:
                    last_error = e
                    # Prepare failures are safe to retry: no update was proposed.
                    await asyncio.sleep(0.1)
                    continue

                # Apply the update only after preparation succeeds, outside the
                # retry handlers. Snapshot it before accept requests can yield.
                new_value = deepcopy(update_fn(current_value))

                try:
                    await self._accept_phase(key, ballot, new_value)
                except (ProposerError, InsufficientQuorumError) as e:
                    # Lost replies and conflicts can both hide accepted values.
                    # Reapplying update_fn to recovered state could apply it twice.
                    raise ProposerError(
                        "INDETERMINATE",
                        "Accept quorum was not confirmed; the update may have been applied",
                    ) from e

                self.value_cache[key] = (ballot, new_value)
                return deepcopy(new_value)
            
            # Exhausted retries
            raise ProposerError(
                "MAX_RETRIES",
                f"Failed after {retries} attempts. Last error: {last_error}"
            )
    
    async def read(self, key: str) -> Optional[Any]:
        """
        Read the current consensus value for a key.
        
        This is essentially a no-op change operation.
        
        Args:
            key: The key to read
            
        Returns:
            The current value or None if unset
        """
        return await self.change(key, lambda v: v)
    
    async def write(self, key: str, value: Any) -> Any:
        """
        Write a value, replacing any existing value.
        
        Args:
            key: The key to write
            value: The value to write
            
        Returns:
            The written value
        """
        return await self.change(key, lambda _: value)
    
    async def compare_and_swap(
        self,
        key: str,
        expected: Any,
        new_value: Any
    ) -> Tuple[bool, Any]:
        """
        Atomic compare-and-swap operation.
        
        Args:
            key: The key to update
            expected: The expected current value
            new_value: The new value to set if expectation matches
            
        Returns:
            Tuple of (matched, resulting_value). matched reports whether the
            current value equaled expected, even when new_value already equals
            the current value. A mismatch leaves the value unchanged.
        """
        matched = False

        def update(current):
            nonlocal matched
            matched = bool(current == expected)
            return new_value if matched else current

        actual_value = await self.change(key, update)
        return matched, actual_value