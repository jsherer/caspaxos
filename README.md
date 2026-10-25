# CASPaxos Python Implementation

A clean, educational Python implementation of the CASPaxos consensus protocol. CASPaxos is a replicated state machine protocol that provides strongly consistent operations without the complexity of leader election and log replication.

## Features

- **Simple API**: Core `change(key, update_fn)` operation for all state modifications
- **No Leader Election**: Symmetric peer-to-peer design avoids single points of failure
- **Optimistic Concurrency**: Compare-and-swap operations for conflict resolution
- **Modular Design**: Separate acceptor, proposer, and storage components
- **Async/Await**: Built on Python's asyncio for concurrent operations
- **In-Memory Storage**: Default in-memory acceptors for development and testing
- **Thread-Safe**: Per-key locking ensures consistency

## Installation

The implementation is self-contained with no external dependencies for the core functionality:

```python
# No installation needed, just import from the caspaxos directory
from caspaxos import CASPaxos, KeyValueStore, CASKeyValue
```

## Quick Start

### Basic Usage

```python
import asyncio
from caspaxos import CASPaxos

async def main():
    # Create CASPaxos instance with 3 acceptors (tolerates 1 failure)
    paxos = CASPaxos(node_id="node1", num_acceptors=3)
    
    # Core operation: change(key, update_function)
    # Set initial value
    await paxos.change("counter", lambda x: 0)
    
    # Increment counter
    result = await paxos.change("counter", lambda x: x + 1)
    print(f"Counter: {result}")  # Counter: 1
    
    # Convenience methods
    await paxos.put("key1", "value1")
    value = await paxos.get("key1")
    print(f"Value: {value}")  # Value: value1

asyncio.run(main())
```

### Key-Value Store

```python
from caspaxos import KeyValueStore

async def main():
    kv = KeyValueStore()
    
    # Store data
    await kv.put("user:1", {"name": "Alice", "age": 30})
    
    # Retrieve data
    user = await kv.get("user:1")
    print(user)  # {'name': 'Alice', 'age': 30}
    
    # Delete data
    await kv.delete("user:1")

asyncio.run(main())
```

### Compare-And-Swap Operations

```python
from caspaxos import CASKeyValue

async def main():
    cas = CASKeyValue()
    
    # Versioned updates
    doc = await cas.read("doc1")
    print(f"Version: {doc['version']}")  # Version: 0
    
    # Update with version check
    await cas.write("doc1", 0, "content v1")
    
    # Atomic counter
    count = await cas.increment("views")
    print(f"Views: {count}")  # Views: 1

asyncio.run(main())
```

## Architecture

### Core Components

1. **BallotNumber** (`core/ballot.py`)
   - Totally ordered ballot numbers using (counter, node_id) pairs
   - Ensures global ordering of proposals

2. **Proposer** (`core/proposer.py`)
   - Coordinates the two-phase consensus protocol
   - Manages prepare and accept phases
   - Maintains per-key locks for serialization

3. **Acceptor** (`storage/acceptor.py`)
   - Stores promised and accepted values
   - Responds to prepare and accept requests
   - In-memory and Redis implementations available

4. **Messages** (`core/messages.py`)
   - PrepareRequest/Response
   - AcceptRequest/Response

### Protocol Flow

```
Client -> Proposer -> Acceptors
           |            |
           |-- Phase 1: Prepare -->
           |<-- Promise + Value ---
           |            |
           |-- Phase 2: Accept --->
           |<-- Accepted ---------
           |
        Result -> Client
```

## Advanced Examples

### Distributed Lock

```python
class DistributedLock:
    def __init__(self, paxos, lock_name):
        self.paxos = paxos
        self.lock_name = f"lock:{lock_name}"
    
    async def acquire(self, owner):
        def try_acquire(current):
            if current is None:
                return {"owner": owner, "time": time.time()}
            raise ValueError("Lock held")
        
        try:
            await self.paxos.change(self.lock_name, try_acquire)
            return True
        except:
            return False
```

### Concurrent Operations

```python
async def concurrent_increments():
    paxos = CASPaxos(num_acceptors=3)
    await paxos.put("counter", 0)
    
    async def increment():
        for _ in range(100):
            await paxos.change("counter", lambda x: x + 1)
    
    # Run 10 workers concurrently
    await asyncio.gather(*[increment() for _ in range(10)])
    
    # Result will be exactly 1000
    final = await paxos.get("counter")
    assert final == 1000
```

## Testing

### Using Make (Recommended)

A Makefile is provided for convenience:

```bash
# Show all available targets
make help

# Run all tests
make test

# Run tests with verbose output
make test-verbose

# Run specific test suites
make test-ballot     # Test ballot number implementation
make test-acceptor   # Test acceptor implementation
make test-proposer   # Test proposer implementation
make test-caspaxos   # Test main CASPaxos API

# Run examples
make example         # or 'make demo'

# Run everything (tests + examples)
make all

# Clean cache files
make clean
```

### Manual Testing

```bash
# Set Python path
export PYTHONPATH=/app

# Run all tests
env/bin/python -m pytest caspaxos/caspaxos/__tests__/

# Run specific test file
env/bin/python -m pytest caspaxos/caspaxos/__tests__/test_ballot.py

# Run with coverage
env/bin/python -m pytest --cov=caspaxos caspaxos/caspaxos/__tests__/
```

### Running Examples

```bash
# Using make
make example

# Or manually
PYTHONPATH=/app env/bin/python -m caspaxos.caspaxos.example
```

## Implementation Notes

### Differences from Original Gryadka

- **Python Async**: Uses asyncio instead of JavaScript promises
- **Type Hints**: Full type annotations for better IDE support
- **Dataclasses**: Message types use Python dataclasses
- **Testing**: pytest-based test suite

### Performance Considerations

- **Memory**: All acceptor state is in-memory by default
- **Concurrency**: Per-key locking allows parallel operations on different keys
- **Caching**: Proposers cache accepted values to avoid unnecessary prepare phases
- **Quorum**: Default majority quorum (N/2 + 1) for N acceptors

### Quorum Completion and Timeouts

Both proposers finish each prepare or accept phase as soon as a quorum succeeds.
They also stop early when the remaining requests cannot form a quorum.
A stalled minority no longer prevents progress.

`CASPaxos`, `Proposer`, and `EnhancedProposer` accept a positive, finite
`phase_timeout` in seconds, defaulting to `5.0`. It bounds each phase, not the
whole change operation; enhanced proposer prepare retries each get a new
deadline. For example, `CASPaxos(phase_timeout=0.5)` uses a half-second deadline
for each prepare or accept phase.

Outstanding requests are cancelled on completion, failure, deadline expiry, or
caller cancellation. Phase completion does not wait for slow cancellation
cleanup; late results and exceptions are collected without changing the returned
outcome. Cancellation is not a rollback: an accept may already have persisted.
Custom acceptors and transports must yield during I/O and honor cancellation to
release resources; requests that delay cancellation remain tracked until they
finish.

### Mutable Values

Values must support `copy.deepcopy`, including independent copies of mutable
contents. Update functions receive detached values and may mutate and return
them. Both proposers snapshot the result before sending accept requests and
return a separate copy, so later caller mutations cannot change a proposal,
the proposer cache, or accepted state.

The in-memory backend snapshots complete acceptor state on write and read,
including ballots and nested values. Custom storage backends must provide the
same ownership isolation. In `CASPaxos`, a callback that mutates its argument
and then raises leaves the previous value unchanged.

### Atomic Acceptor Transitions

Both acceptor implementations use `StorageBackend.update_state(key, transition)`
to read state, evaluate the protocol rule, and persist the result atomically.
The synchronous transition receives a detached state snapshot and returns
`(replacement, result)`; a `None` replacement means no write. A transition that
raises must leave stored state unchanged.

Custom backends must implement this operation and serialize it against other
writes to the same stored key, including access through multiple acceptors.
Separate `get_state` and `set_state` calls are not an atomic transition.
Persistent backends need a transaction or equivalent conditional update.

The in-memory backend holds a per-key asyncio lock across the transition and
snapshot write; unrelated keys remain independent. Locks are retained across
deletion and clearing so queued operations keep using the same lock.

### Failed or Cancelled Writes

`CASPaxos` / `Proposer` discard the cached round before sending accept requests
and restore the cache only after a quorum succeeds. If acceptance fails or the
operation is cancelled, the next operation runs a fresh prepare phase instead
of reusing the uncertain round's ballot for a different value.

A failed or cancelled write may already have reached some acceptors; it is not
a rollback guarantee. Retrying a non-idempotent update is not guaranteed to
apply it exactly once.

`EnhancedProposer.change(..., retries=10)` retries only preparation, before
applying the update or sending accept requests. If an accept quorum cannot be
confirmed, it raises `caspaxos.core.proposer_enhanced.ProposerError` with
`code == "INDETERMINATE"` and does not reapply the update. This includes accept
conflicts: other replicas may already have accepted the proposed value.
A confirmed quorum still succeeds when a minority of responses are lost.

An indeterminate outcome does not mean the write failed. Recover with a
consistent read and application-specific reconciliation rather than blindly
repeating a non-idempotent update. Exactly-once retries require application-level
deduplication.

### Limitations

- **Single Machine**: Current implementation runs on a single machine
- **No Persistence**: In-memory acceptors lose state on restart
- **No Network**: Acceptors communicate via method calls, not network
- **No Reconfiguration**: Static acceptor membership

## Future Enhancements

1. **Redis Backend**: Persistent acceptor state using Redis
2. **Network Communication**: TCP/HTTP based acceptor protocol
3. **Cluster Membership**: Dynamic addition/removal of acceptors
4. **Monitoring**: Metrics and observability
5. **Optimizations**: Batching, pipelining, and lease-based reads

## References

- [CASPaxos Paper (arXiv:1802.07000)](https://arxiv.org/abs/1802.07000)
- [Gryadka - JavaScript Implementation](https://github.com/gryadka/js)
- [Paxos Made Simple](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf)

## License

See [LICENSE](LICENSE)

## Contributing

This is an educational implementation. Contributions that improve clarity, correctness, or educational value are welcome!

1. Write tests for new features
2. Maintain clean, readable code
3. Add docstrings and type hints
4. Update documentation
