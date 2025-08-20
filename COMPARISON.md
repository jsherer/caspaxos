# CASPaxos Implementation Comparison

This document compares our Python implementation with Peter Bourgon's Go implementation.

## Overview

Both implementations follow the CASPaxos paper closely but make different design choices based on language idioms and intended use cases.

## Core Components Comparison

### 1. Ballot Numbers

#### Go Implementation (Bourgon)
```go
type Ballot struct {
    Counter uint64
    ID      string
}

func (b *Ballot) greaterThan(other Ballot) bool
func (b *Ballot) inc() Ballot
func (b *Ballot) isZero() bool
```

#### Python Implementation (Ours)
```python
class BallotNumber:
    counter: int
    node_id: str
    
    def compare_to(self, other) -> int
    def increment(self) -> BallotNumber
    def is_zero(self) -> bool
```

**Analysis**: Both implementations are nearly identical. Main differences:
- Go uses `uint64`, Python uses `int` (unbounded)
- Go uses `ID`, Python uses `node_id` for clarity
- Python implements full comparison operators (`__lt__`, `__gt__`, etc.)
- Python adds serialization methods (`stringify()`, `parse()`)

### 2. Proposer

#### Go Implementation
```go
type MemoryProposer struct {
    mtx       sync.Mutex
    id        string
    age       Age  // For garbage collection
    ballot    Ballot
    preparers map[string]Preparer
    accepters map[string]Accepter
    watchers  map[string]StateWatcher
}

func (p *MemoryProposer) Propose(ctx context.Context, key string, f ChangeFunc) (state []byte, b Ballot, err error)
```

#### Python Implementation
```python
class Proposer:
    node_id: str
    ballot: BallotNumber
    acceptors: List[Acceptor]
    quorum_size: int
    cache: Dict[str, Tuple[BallotNumber, Any]]
    locks: Dict[str, asyncio.Lock]
    
    async def change(key: str, update_fn: Callable, extra: Any = None) -> Any
```

**Key Differences**:
- **Concurrency**: Go uses mutex, Python uses asyncio locks per key
- **Age/GC**: Go implements age-based garbage collection, Python doesn't
- **Cache**: Python caches accepted values, Go doesn't explicitly cache
- **Watchers**: Go has state watchers, Python doesn't
- **Separation**: Go separates preparers/accepters, Python uses unified acceptors

### 3. Acceptor

#### Go Implementation
```go
type MemoryAcceptor struct {
    mtx      sync.Mutex
    addr     string
    ages     map[string]Age
    values   map[string]acceptedValue
    watchers watchers
}

type acceptedValue struct {
    promise  Ballot
    accepted Ballot
    value    []byte
}
```

#### Python Implementation
```python
class InMemoryAcceptor:
    node_id: str
    states: Dict[str, AcceptorState]
    locks: Dict[str, asyncio.Lock]

class AcceptorState:
    promised_ballot: BallotNumber
    accepted_ballot: BallotNumber
    accepted_value: Optional[Any]
```

**Key Differences**:
- **Per-key locking**: Python uses per-key async locks, Go uses single mutex
- **Age tracking**: Go tracks ages for GC, Python doesn't
- **State organization**: Similar structure but Python separates state class

## Protocol Implementation

### Phase 1: Prepare

#### Go Implementation
- Broadcasts to all preparers concurrently
- Collects results through channels
- Fast-forwards ballot on conflicts
- Supports full quorum for configuration changes

#### Python Implementation
- Sends to all acceptors using asyncio.gather
- Collects results as list
- Fast-forwards ballot on conflicts
- Fixed quorum calculation (majority)

### Phase 2: Accept

#### Go Implementation
- Separate accept phase with quorum counting
- Clear promise after accept
- Returns detailed conflict information

#### Python Implementation
- Similar accept phase
- Updates promise to next ballot
- Simpler error handling

## API Design

### Go Implementation
```go
// Function-based change
type ChangeFunc func(current []byte) (new []byte)
proposer.Propose(ctx, key, changeFunc)
```

### Python Implementation
```python
# Function-based change with convenience methods
async def change(key, update_fn) -> Any
async def get(key) -> Any  # Identity function
async def put(key, value) -> Any  # Constant function

# Higher-level abstractions
KeyValueStore()  # Dictionary-like API
CASKeyValue()    # Compare-and-swap operations
```

## Advanced Features Comparison

| Feature | Go (Bourgon) | Python (Ours) |
|---------|--------------|---------------|
| **Age-based GC** | ✅ Implemented | ❌ Not implemented |
| **Configuration changes** | ✅ Full/regular quorum | ❌ Not implemented |
| **State watchers** | ✅ StateWatcher interface | ❌ Not implemented |
| **Persistence** | ❌ Memory only | ❌ Memory only |
| **Network transport** | ✅ HTTP API available | ❌ Not implemented |
| **Cluster management** | ✅ Basic implementation | ❌ Not implemented |
| **Optimizations** | ✅ Key affinity, retries | ✅ Value caching |
| **Testing** | ✅ Comprehensive | ✅ Comprehensive |

## Language-Specific Design Choices

### Go Implementation
- Uses channels for concurrent result collection
- Context for cancellation
- Interfaces for pluggable components
- Error types with detailed information
- Separate packages for protocol/cluster/API

### Python Implementation
- Uses asyncio for async/await pattern
- Dataclasses for messages
- ABC for base classes
- Exceptions for error handling
- Single package with submodules

## Strengths and Weaknesses

### Go Implementation Strengths
- Production-oriented with HTTP API
- Age-based garbage collection
- Configuration change support
- More complete distributed system features
- Better separation of concerns

### Go Implementation Weaknesses
- More complex codebase
- No high-level abstractions (KV store, CAS)
- Less convenient API for simple use cases

### Python Implementation Strengths
- Cleaner, more educational code
- High-level abstractions (KeyValueStore, CASKeyValue)
- Better documentation and examples
- Simpler to understand and use
- Type hints throughout

### Python Implementation Weaknesses
- No network transport
- No cluster management
- No age-based GC
- No configuration change protocol
- Less production-ready

## Recommendations for Python Implementation

Based on the Go implementation analysis, our Python implementation could benefit from:

1. **Age-based Garbage Collection**: Add age tracking to prevent old proposals
2. **Configuration Changes**: Implement AddAcceptor/RemoveAcceptor with proper quorum
3. **State Watchers**: Add observer pattern for state changes
4. **Network Layer**: Add gRPC or HTTP transport
5. **Persistence**: Add optional persistent storage backends
6. **Cluster Management**: Basic node discovery and health checking

## Implementation Correctness

### Protocol Adherence
Both implementations correctly follow the CASPaxos paper:
- ✅ Two-phase protocol (Prepare/Accept)
- ✅ Ballot number ordering and uniqueness
- ✅ Quorum-based consensus
- ✅ Conflict detection and ballot fast-forwarding
- ✅ Change function application

### Key Implementation Details

#### Retry Logic
- **Go**: Automatic single retry on prepare failure to hide fast-forwards
- **Python**: No automatic retry, errors bubble up to caller

#### Ballot Increment
- **Go**: Increments on every attempt (critical for correctness per Rystsov)
- **Python**: Increments on every attempt (same)

#### Quorum Calculation
- **Go**: Flexible quorum functions (regular: F+1, full: 2F+1)
- **Python**: Fixed majority quorum ((N/2)+1)

#### Concurrency Model
- **Go**: Single mutex for proposer, goroutines for parallel requests
- **Python**: Per-key locks, asyncio.gather for parallel requests

## Performance Considerations

### Go Implementation
- Single proposer lock may limit throughput
- Goroutines provide lightweight concurrency
- Channel-based result collection
- No explicit caching of values

### Python Implementation
- Per-key locking allows better parallelism
- Async/await has more overhead than goroutines
- List-based result collection
- Explicit value caching reduces prepare phases

## Conclusion

Both implementations correctly implement the core CASPaxos protocol. The Go implementation is more production-oriented with additional distributed systems features, while the Python implementation is cleaner and more educational with better high-level abstractions. 

**Key Takeaways**:
1. Both follow the paper correctly with proper ballot management
2. Go focuses on production features (GC, config changes, network)
3. Python focuses on simplicity and usability (high-level APIs)
4. Python's per-key locking may offer better concurrency
5. Go's retry logic provides better user experience
6. Both need persistence for production use

The Python version would benefit from:
- Automatic retry logic like Go
- Age-based GC for long-running systems
- Configuration change protocol
- Network transport layer