"""
Base acceptor interface and state management for CASPaxos.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional
from ..core.ballot import BallotNumber
from ..core.messages import PrepareRequest, PrepareResponse, AcceptRequest, AcceptResponse


@dataclass
class AcceptorState:
    """
    State maintained by an acceptor for a single key.
    
    Attributes:
        promised_ballot: Highest ballot promised not to accept lower ballots
        accepted_ballot: Ballot of the last accepted value
        accepted_value: The last accepted value
    """
    promised_ballot: BallotNumber
    accepted_ballot: BallotNumber
    accepted_value: Optional[Any] = None
    
    @classmethod
    def initial(cls) -> "AcceptorState":
        """Create initial acceptor state."""
        return cls(
            promised_ballot=BallotNumber.zero(),
            accepted_ballot=BallotNumber.zero(),
            accepted_value=None
        )


class Acceptor(ABC):
    """
    Abstract base class for CASPaxos acceptors.
    
    An acceptor participates in the consensus protocol by responding to
    prepare and accept requests from proposers.
    """
    
    @abstractmethod
    async def prepare(self, request: PrepareRequest) -> PrepareResponse:
        """
        Handle a prepare request.
        
        Phase 1 of CASPaxos: Check if we can promise not to accept
        any ballot lower than the requested one.
        
        Args:
            request: The prepare request from proposer
            
        Returns:
            PrepareResponse indicating success or failure
        """
        pass
    
    @abstractmethod
    async def accept(self, request: AcceptRequest) -> AcceptResponse:
        """
        Handle an accept request.
        
        Phase 2 of CASPaxos: Accept the value if the ballot is still valid.
        
        Args:
            request: The accept request from proposer
            
        Returns:
            AcceptResponse indicating success or failure
        """
        pass
    
    @abstractmethod
    async def get_state(self, key: str) -> Optional[AcceptorState]:
        """
        Get the current state for a key.
        
        Args:
            key: The key to get state for
            
        Returns:
            Current acceptor state or None if key doesn't exist
        """
        pass