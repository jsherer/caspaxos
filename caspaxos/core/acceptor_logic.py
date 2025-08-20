"""
Pure protocol logic for CASPaxos acceptors.

This module contains the stateless protocol logic for processing prepare
and accept requests, independent of storage concerns.
"""

from dataclasses import dataclass
from typing import Optional, Tuple
from .ballot import BallotNumber
from .messages import PrepareRequest, PrepareResponse, AcceptRequest, AcceptResponse
from ..storage.acceptor import AcceptorState


@dataclass
class StateUpdate:
    """
    Represents a state update to be applied.
    
    Attributes:
        state: The new state to store
        should_update: Whether the state should be updated
    """
    state: AcceptorState
    should_update: bool


class AcceptorLogic:
    """
    Stateless protocol logic for CASPaxos acceptors.
    
    This class contains pure functions that implement the CASPaxos
    protocol rules, without any storage or I/O operations.
    """
    
    @staticmethod
    def process_prepare(
        current_state: Optional[AcceptorState],
        request: PrepareRequest
    ) -> Tuple[StateUpdate, PrepareResponse]:
        """
        Process a prepare request according to CASPaxos protocol.
        
        This is a pure function that takes the current state and a request,
        and returns the new state and response without side effects.
        
        Args:
            current_state: Current acceptor state (None if new key)
            request: The prepare request to process
            
        Returns:
            Tuple of (StateUpdate, PrepareResponse)
        """
        # Initialize state if needed
        if current_state is None:
            current_state = AcceptorState.initial()
        
        # Check if we can promise this ballot
        can_promise = (
            request.ballot > current_state.promised_ballot and
            request.ballot > current_state.accepted_ballot
        )
        
        if can_promise:
            # Create new state with updated promise
            new_state = AcceptorState(
                promised_ballot=request.ballot,
                accepted_ballot=current_state.accepted_ballot,
                accepted_value=current_state.accepted_value
            )
            
            # Return success response with accepted value (if any)
            response = PrepareResponse(
                is_prepared=True,
                ballot=current_state.accepted_ballot,
                value=current_state.accepted_value
            )
            
            return StateUpdate(new_state, should_update=True), response
        else:
            # Return conflict with highest ballot we've seen
            highest_ballot = max(
                current_state.promised_ballot,
                current_state.accepted_ballot
            )
            
            response = PrepareResponse(
                is_conflict=True,
                ballot=highest_ballot
            )
            
            # No state update needed
            return StateUpdate(current_state, should_update=False), response
    
    @staticmethod
    def process_accept(
        current_state: Optional[AcceptorState],
        request: AcceptRequest
    ) -> Tuple[StateUpdate, AcceptResponse]:
        """
        Process an accept request according to CASPaxos protocol.
        
        This is a pure function that takes the current state and a request,
        and returns the new state and response without side effects.
        
        Args:
            current_state: Current acceptor state (None if new key)
            request: The accept request to process
            
        Returns:
            Tuple of (StateUpdate, AcceptResponse)
        """
        # Initialize state if needed
        if current_state is None:
            current_state = AcceptorState.initial()
        
        # Check if we can accept this ballot
        can_accept = (
            request.ballot >= current_state.promised_ballot and
            request.ballot > current_state.accepted_ballot
        )
        
        if can_accept:
            # Create new state with accepted value
            new_state = AcceptorState(
                promised_ballot=request.promise,
                accepted_ballot=request.ballot,
                accepted_value=request.value
            )
            
            response = AcceptResponse(is_ok=True)
            
            return StateUpdate(new_state, should_update=True), response
        else:
            # Return conflict with highest ballot we've seen
            highest_ballot = max(
                current_state.promised_ballot,
                current_state.accepted_ballot
            )
            
            response = AcceptResponse(
                is_conflict=True,
                ballot=highest_ballot
            )
            
            # No state update needed
            return StateUpdate(current_state, should_update=False), response