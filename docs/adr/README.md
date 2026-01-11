# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for the Helix project.

ADRs document significant architectural decisions along with their context and consequences. They serve as a log of what we decided, why we decided it, and what alternatives we considered.

## Index

| ADR | Title | Status |
|-----|-------|--------|
| [0000](0000-template.md) | ADR Template | - |
| [0001](0001-raft-for-partition-replication.md) | Use Raft for Partition Replication | Accepted |
| [0002](0002-synchronous-state-machines.md) | Synchronous State Machines in Core | Accepted |
| [0003](0003-strongly-typed-identifiers.md) | Strongly-Typed Identifiers | Accepted |
| [0004](0004-tick-based-raft-timing.md) | Tick-Based Raft Timing | Accepted |
| [0005](0005-conservative-wal-corruption-recovery.md) | Conservative WAL Corruption Recovery | Accepted |

## When to Write an ADR

Write an ADR when:
- Choosing between multiple viable approaches
- Making a decision that would be costly to reverse
- The decision affects multiple components or the overall architecture
- Future developers will ask "why did we do it this way?"

Do NOT write an ADR for:
- Implementation details that can easily change
- Bug fixes
- Routine refactoring
- Decisions already well-documented elsewhere

## ADR Lifecycle

1. **Proposed**: Initial draft, open for discussion
2. **Accepted**: Decision is final and being implemented
3. **Deprecated**: No longer applies but kept for history
4. **Superseded**: Replaced by a newer ADR (link to replacement)

## Format

We use a MADR-inspired format with emphasis on:
- **Context**: The forces at play
- **Decision**: What we chose
- **Consequences**: The tradeoffs we accepted
- **Options Considered**: Alternatives with pros/cons

See [0000-template.md](0000-template.md) for the full template.

## References

- [ADR GitHub Organization](https://adr.github.io/)
- [MADR Template](https://adr.github.io/madr/)
- [Documenting Architecture Decisions](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
