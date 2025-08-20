"""
Proposer implementation for CASPaxos protocol.

The proposer coordinates the two-phase consensus protocol, managing
prepare and accept phases to achieve consensus on values.
"""

import asyncio
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from .ballot import BallotNumber
from .messages import PrepareRequest, PrepareResponse, AcceptRequest, AcceptResponse
from ..storage.acceptor import Acceptor


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


class Proposer:
    """
    CASPaxos proposer implementation.
    
    The proposer coordinates the consensus protocol by:
    1. Sending prepare requests to acceptors (Phase 1)
    2. Applying the update function to the latest value
    3. Sending accept requests with the new value (Phase 2)
    """
    
    def __init__(
        self,
        node_id: str,
        acceptors: List[Acceptor],
        quorum_size: Optional[int] = None
    ):
        """
        Initialize the proposer.
        
        Args:
            node_id: Unique identifier for this proposer
            acceptors: List of acceptor instances
            quorum_size: Size of quorum needed (default: majority)
        """
        self.node_id = node_id
        self.acceptors = acceptors
        self.quorum_size = quorum_size or (len(acceptors) // 2 + 1)
        
        if self.quorum_size > len(acceptors):
            raise ValueError(f"Quorum size {self.quorum_size} > acceptors {len(acceptors)}")
        
        # Current ballot for this proposer
        self.ballot = BallotNumber(0, node_id)
        
        # Cache of accepted values per key
        self.cache: Dict[str, Tuple[BallotNumber, Any]] = {}
        
        # Locks for concurrent operations on same key
        self.locks: Dict[str, asyncio.Lock] = {}
    
    def _get_lock(self, key: str) -> asyncio.Lock:
        """Get or create a lock for a key."""
        if key not in self.locks:
            self.locks[key] = asyncio.Lock()
        return self.locks[key]
    
    async def change(
        self,
        key: str,
        update_fn: Callable[[Optional[Any]], Any],
        extra: Optional[Any] = None
    ) -> Any:
        """
        Execute a change operation using CASPaxos.
        
        This is the main entry point for proposers. It:
        1. Acquires a lock for the key
        2. Runs prepare phase to get current value
        3. Applies update function
        4. Runs accept phase with new value
        
        Args:
            key: The key to operate on
            update_fn: Function to compute new value from current value
            extra: Optional extra data for protocol extensions
            
        Returns:
            The new value after applying update_fn
            
        Raises:
            ProposerError: If consensus cannot be reached
        """
        async with self._get_lock(key):
            try:
                # Phase 1: Prepare
                ballot, current_value = await self._prepare_phase(key, extra)
                
                # Apply update function
                new_value = None
                update_error = None
                try:
                    new_value = update_fn(current_value)
                except Exception as e:
                    update_error = e
                    new_value = current_value  # Keep current value on error
                
                # Get promise ballot for next round
                promise = ballot.next()
                
                # Phase 2: Accept
                await self._accept_phase(key, ballot, new_value, promise, extra)
                
                # Cache the accepted value
                self.cache[key] = (promise, new_value)
                
                # Raise update error after committing (if any)
                if update_error:
                    raise ProposerError("UpdateError", str(update_error))
                
                return new_value
                
            except ProposerError:
                raise
            except Exception as e:
                raise ProposerError("UnexpectedError", str(e))
    
    async def _prepare_phase(
        self,
        key: str,
        extra: Optional[Any] = None
    ) -> Tuple[BallotNumber, Optional[Any]]:
        """
        Execute prepare phase (Phase 1) of CASPaxos.
        
        Args:
            key: The key to prepare
            extra: Optional extra data
            
        Returns:
            Tuple of (ballot, current_value)
            
        Raises:
            ProposerError: If prepare phase fails
        """
        # Check cache first
        if key in self.cache:
            return self.cache[key]
        
        # Increment ballot for this round
        self.ballot = self.ballot.increment()
        
        # Send prepare requests to all acceptors
        prepare_requests = [
            acceptor.prepare(PrepareRequest(key, self.ballot, extra))
            for acceptor in self.acceptors
        ]
        
        # Wait for quorum of responses
        responses = await self._wait_for_quorum(
            prepare_requests,
            lambda r: r.is_prepared
        )
        
        # Find the value with highest ballot
        highest_ballot = BallotNumber.zero()
        accepted_value = None
        
        for response in responses:
            if response.ballot and response.ballot > highest_ballot:
                highest_ballot = response.ballot
                accepted_value = response.value
        
        # Cache and return
        self.cache[key] = (self.ballot, accepted_value)
        return self.ballot, accepted_value
    
    async def _accept_phase(
        self,
        key: str,
        ballot: BallotNumber,
        value: Any,
        promise: BallotNumber,
        extra: Optional[Any] = None
    ) -> None:
        """
        Execute accept phase (Phase 2) of CASPaxos.
        
        Args:
            key: The key to accept
            ballot: The ballot number for this proposal
            value: The value to accept
            promise: The promise ballot for next round
            extra: Optional extra data
            
        Raises:
            ProposerError: If accept phase fails
        """
        # Send accept requests to all acceptors
        accept_requests = [
            acceptor.accept(AcceptRequest(key, ballot, value, promise, extra))
            for acceptor in self.acceptors
        ]
        
        try:
            # Wait for quorum of successful responses
            await self._wait_for_quorum(
                accept_requests,
                lambda r: r.is_ok
            )
        except InsufficientQuorumError as e:
            # Check for conflicts and fast-forward ballot if needed
            for response in e.responses:
                if hasattr(response, 'is_conflict') and response.is_conflict and response.ballot:
                    self.cache.pop(key, None)  # Invalidate cache
                    self.ballot.fastforward_after(response.ballot)
            raise ProposerError("CommitError", "Failed to reach consensus")
    
    async def _wait_for_quorum(
        self,
        requests: List,
        success_predicate: Callable[[Any], bool]
    ) -> List[Any]:
        """
        Wait for a quorum of successful responses.
        
        Args:
            requests: List of async requests to wait for
            success_predicate: Function to check if response is successful
            
        Returns:
            List of successful responses
            
        Raises:
            InsufficientQuorumError: If quorum cannot be reached
        """
        # Gather all responses, handling errors
        responses = await asyncio.gather(*requests, return_exceptions=True)
        
        # Separate successful and failed responses
        successful = []
        all_responses = []
        
        for response in responses:
            if isinstance(response, Exception):
                continue  # Skip exceptions
            
            all_responses.append(response)
            if success_predicate(response):
                successful.append(response)
        
        # Check if we have quorum
        if len(successful) >= self.quorum_size:
            return successful
        
        # Handle conflicts by checking conflicting ballots
        for response in all_responses:
            if hasattr(response, 'is_conflict') and response.is_conflict and response.ballot:
                self.ballot.fastforward_after(response.ballot)
        
        raise InsufficientQuorumError(all_responses)