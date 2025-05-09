# WAL Implementation Plan for BayunDB

## Overview

This document outlines the implementation plan for adding Write-Ahead Logging (WAL) to BayunDB, providing crash recovery and supporting ACID transactions.

## Goals

- Ensure database durability through crash recovery
- Support atomic transactions
- Maintain performance with minimal overhead
- Provide foundations for transaction isolation

## Design and Architecture

### WAL Components

- **Log Manager**: Core component handling log writing and recovery
- **Log Record**: Data structure for different operations (insert, update, delete, etc.)
- **Log Buffer**: In-memory buffer before flushing to disk
- **Checkpoint Manager**: Handles periodic checkpoints to reduce recovery time

### Directory Structure

```
src/
└── transaction/
    ├── wal/
    │   ├── log_manager.rs  
    │   ├── log_record.rs
    │   ├── log_buffer.rs
    │   ├── checkpoint.rs
    │   └── recovery.rs
    ├── concurrency/        (Future implementation)
    └── transaction.rs      (Transaction manager)
```

## Log Record Format

Each log record will contain:

1. **Header**:
   - Log Sequence Number (LSN): Unique identifier for each log record
   - Transaction ID (TXN_ID): Identifies the transaction that created this record
   - Previous LSN: Links to the previous log record for the same transaction
   - Record Type: Identifies the operation (INSERT, UPDATE, DELETE, BEGIN, COMMIT, ABORT)
   - Record Length: Size of the log record content

2. **Content (depends on record type)**:
   - For Data Operations (INSERT/UPDATE/DELETE):
     - Table ID
     - Page ID
     - Record ID
     - Before-image (for UPDATE/DELETE)
     - After-image (for INSERT/UPDATE)
   - For Transaction Operations (BEGIN/COMMIT/ABORT):
     - Timestamp
     - Additional metadata

## Implementation Steps

### Phase 1: Core WAL Infrastructure (Weeks 1-2)

1. **Define Log Record Format**
   - Create `LogRecordType` enum
   - Implement `LogRecord` struct with serialization/deserialization
   - Add LSN generation and tracking
   - Write unit tests for each record type

2. **Implement Log Buffer**
   - Create fixed-size in-memory circular buffer
   - Implement thread-safe append operations
   - Add buffer flushing mechanism with configurable policies
   - Implement force flush on transaction commit

3. **Build Log Manager**
   - Design log file format with headers and segments
   - Implement sequential disk writing
   - Add log file rotation when size threshold is reached
   - Implement log truncation based on checkpoints

### Phase 2: Integration (Weeks 3-4)

1. **Modify Buffer Pool Manager**
   - Add page LSN tracking
   - Implement force-write policy after commit
   - Ensure updates are logged before page modifications
   - Add recovery page table

2. **Connect to Page Operations**
   - Modify page operations to generate log records
   - Track dirty pages for recovery
   - Add before/after image capturing

3. **Add Checkpointing**
   - Implement fuzzy checkpointing
   - Create algorithm for determining checkpoint frequency
   - Handle buffer pool flushing during checkpoint

### Phase 3: Recovery (Weeks 5-6)

1. **Analysis Phase**
   - Scan log to identify active transactions at crash time
   - Build dirty page table and recovery info

2. **Redo Phase**
   - Replay all actions from last checkpoint
   - Restore database state as of crash time
   - Handle partial page updates

3. **Undo Phase**
   - Roll back uncommitted transactions
   - Clean up incomplete operations
   - Restore database to consistent state

## Testing Strategy

1. **Unit Tests**
   - Test log record serialization/deserialization
   - Verify log buffer operations and concurrency
   - Test log manager disk operations
   - Verify checkpoint mechanisms

2. **Integration Tests**
   - Test end-to-end logging for different operations
   - Verify database consistency after simulated crashes
   - Test crash recovery at different points in transaction
   - Test recovery with multiple concurrent transactions

3. **Performance Tests**
   - Measure throughput with WAL enabled vs. disabled
   - Test recovery time with different log sizes/checkpoint frequencies
   - Benchmark different flushing policies
   - Measure space overhead

## Key Interfaces

### Log Record

```rust
enum LogRecordType {
    Begin,
    Commit,
    Abort,
    Update,
    Insert,
    Delete,
    Checkpoint,
}

struct LogRecord {
    lsn: u64,
    txn_id: u32,
    prev_lsn: u64,
    record_type: LogRecordType,
    content: Vec<u8>,
}

impl LogRecord {
    fn serialize(&self) -> Vec<u8>;
    fn deserialize(data: &[u8]) -> Result<Self>;
}
```

### Log Manager

```rust
struct LogManager {
    log_buffer: LogBuffer,
    log_file: File,
    current_lsn: u64,
    // Other fields
}

impl LogManager {
    fn new(buffer_size: usize, log_path: &str) -> Result<Self>;
    fn append_log_record(&mut self, txn_id: u32, prev_lsn: u64, 
                         record_type: LogRecordType, content: &[u8]) -> Result<u64>;
    fn flush(&mut self) -> Result<()>;
    fn flush_till_lsn(&mut self, lsn: u64) -> Result<()>;
    fn recover() -> Result<()>;
    fn checkpoint() -> Result<()>;
}
```

### Integration with Buffer Pool

```rust
impl BufferPoolManager {
    // Add new methods
    fn pin_page_with_lsn(&mut self, page_id: PageId, lsn: u64) -> Result<Arc<Page>>;
    fn mark_page_dirty_with_lsn(&mut self, page_id: PageId, lsn: u64) -> Result<()>;
    
    // Modify existing methods to work with WAL
    fn flush_page(&mut self, page_id: PageId) -> Result<()>;
    fn flush_all_pages(&mut self) -> Result<()>;
}
```

## Important Considerations

- **Atomicity**: Ensure log records are written atomically to disk
- **Performance**: Optimize log buffer flushing policies for throughput
- **Durability**: Provide configurable durability guarantees
- **Concurrency**: Design for multi-threaded transaction processing
- **Storage Efficiency**: Implement log compaction and archiving
- **Recovery Performance**: Balance checkpoint frequency with recovery time

## Timeline and Milestones

1. **Week 1**: Design and implement log records and buffer
2. **Week 2**: Implement log manager and disk operations
3. **Week 3**: Integrate with buffer pool and page operations
4. **Week 4**: Implement checkpointing
5. **Week 5**: Implement recovery mechanisms
6. **Week 6**: Testing and performance tuning

## Success Criteria

1. All tests pass, including crash recovery tests
2. Performance overhead is less than 15% compared to non-WAL operations
3. Recovery time is proportional to the amount of work since last checkpoint
4. The system correctly recovers to a consistent state after simulated crashes 