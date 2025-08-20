"""
Ballot number implementation for CASPaxos.

A ballot number consists of a counter and a node ID, providing total ordering
of ballots across the distributed system.
"""

from typing import Optional, Tuple


class BallotNumber:
    """
    Represents a ballot number in the CASPaxos protocol.
    
    Ballots are totally ordered by (counter, node_id) pairs.
    """
    
    def __init__(self, counter: int = 0, node_id: str = ""):
        """
        Initialize a ballot number.
        
        Args:
            counter: The ballot counter (higher means more recent)
            node_id: The unique identifier of the proposer node
        """
        self.counter = counter
        self.node_id = node_id
    
    @classmethod
    def zero(cls) -> "BallotNumber":
        """Create a zero ballot (initial state)."""
        return cls(0, "")
    
    @classmethod
    def parse(cls, text: str) -> "BallotNumber":
        """
        Parse a ballot from string format "counter,node_id".
        
        Args:
            text: String representation of ballot
            
        Returns:
            Parsed BallotNumber instance
        """
        parts = text.split(",", 1)
        counter = int(parts[0])
        node_id = parts[1] if len(parts) > 1 else ""
        return cls(counter, node_id)
    
    def is_zero(self) -> bool:
        """Check if this is a zero ballot."""
        return self.counter == 0 and self.node_id == ""
    
    def increment(self) -> "BallotNumber":
        """
        Increment the ballot counter and return new ballot.
        
        Returns:
            New BallotNumber with incremented counter
        """
        return BallotNumber(self.counter + 1, self.node_id)
    
    def next(self) -> "BallotNumber":
        """
        Get the next ballot number (counter + 1).
        
        Returns:
            New BallotNumber with counter increased by 1
        """
        return BallotNumber(self.counter + 1, self.node_id)
    
    def fastforward_after(self, other: "BallotNumber") -> None:
        """
        Fast-forward this ballot to be after another ballot.
        
        Updates counter to be at least one more than the other ballot's counter.
        
        Args:
            other: The ballot to fast-forward past
        """
        self.counter = max(self.counter, other.counter) + 1
    
    def stringify(self) -> str:
        """
        Convert ballot to string format "counter,node_id".
        
        Returns:
            String representation of ballot
        """
        return f"{self.counter},{self.node_id}"
    
    def compare_to(self, other: "BallotNumber") -> int:
        """
        Compare this ballot with another.
        
        Args:
            other: Ballot to compare with
            
        Returns:
            -1 if this < other, 0 if equal, 1 if this > other
        """
        if self.counter < other.counter:
            return -1
        if self.counter > other.counter:
            return 1
        if self.node_id < other.node_id:
            return -1
        if self.node_id > other.node_id:
            return 1
        return 0
    
    def __lt__(self, other: "BallotNumber") -> bool:
        """Less than comparison."""
        return self.compare_to(other) < 0
    
    def __le__(self, other: "BallotNumber") -> bool:
        """Less than or equal comparison."""
        return self.compare_to(other) <= 0
    
    def __gt__(self, other: "BallotNumber") -> bool:
        """Greater than comparison."""
        return self.compare_to(other) > 0
    
    def __ge__(self, other: "BallotNumber") -> bool:
        """Greater than or equal comparison."""
        return self.compare_to(other) >= 0
    
    def __eq__(self, other: object) -> bool:
        """Equality comparison."""
        if not isinstance(other, BallotNumber):
            return False
        return self.compare_to(other) == 0
    
    def __ne__(self, other: object) -> bool:
        """Inequality comparison."""
        return not self.__eq__(other)
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"BallotNumber({self.counter}, '{self.node_id}')"
    
    def __str__(self) -> str:
        """String conversion."""
        return self.stringify()
    
    def __hash__(self) -> int:
        """Hash for use in sets/dicts."""
        return hash((self.counter, self.node_id))