# ADR-0003: Strongly-Typed Identifiers

## Status

Accepted

## Context

Helix uses many different numeric identifiers and positions:
- `Offset` - position in a partition log
- `LogIndex` - position in Raft log
- `TermId` - Raft election term
- `NodeId`, `TopicId`, `PartitionId` - entity identifiers
- `ConsumerId`, `ConsumerGroupId` - consumer identifiers

These are all `u64` under the hood, making it easy to accidentally:
- Pass an `Offset` where a `LogIndex` is expected
- Compare incompatible values
- Perform invalid arithmetic (e.g., adding two offsets)

## Decision

Use newtype wrappers for all identifiers and positions:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Offset(u64);

impl Offset {
    pub fn new(value: u64) -> Self { Self(value) }
    pub fn get(self) -> u64 { self.0 }
    pub fn next(self) -> Self { Self(self.0 + 1) }
}
```

The types intentionally do NOT implement `Add`, `Sub`, etc. to prevent accidental arithmetic. When arithmetic is needed, use explicit `.get()` and wrap back.

## Consequences

**Easier:**
- Compiler catches type mismatches
- Self-documenting function signatures
- IDE autocomplete shows meaningful types
- Prevents entire class of bugs at compile time

**Harder:**
- More verbose when arithmetic is actually needed
- Must convert at API boundaries
- Slightly more boilerplate for new ID types

## Options Considered

### Option 1: Newtype Wrappers (Chosen)

**Pros:**
- Zero runtime cost (same as raw u64)
- Compile-time safety
- Clear semantics in function signatures

**Cons:**
- Boilerplate for each type
- Explicit conversion needed

### Option 2: Type Aliases

```rust
type Offset = u64;
type LogIndex = u64;
```

**Pros:**
- No boilerplate
- Full numeric operations

**Cons:**
- No type safety (aliases are interchangeable)
- Easy to mix up
- Defeats the purpose

### Option 3: Generic ID Type

```rust
struct Id<T>(u64, PhantomData<T>);
type Offset = Id<OffsetMarker>;
```

**Pros:**
- Single implementation for all IDs
- Consistent behavior

**Cons:**
- More complex generic signatures
- Phantom data adds cognitive overhead
- Harder to add type-specific methods

### Option 4: Macro-Generated Types

**Pros:**
- Reduces boilerplate
- Consistent implementation

**Cons:**
- Macro complexity
- Harder to customize per-type
- IDE support can be worse

## References

- [Rust Newtype Pattern](https://doc.rust-lang.org/rust-by-example/generics/new_types.html)
- [Parse, Don't Validate](https://lexi-lambda.github.io/blog/2019/11/05/parse-don-t-validate/)
- [TigerStyle on Explicit Types](./../../TIGER_STYLE.md)
