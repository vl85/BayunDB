# B+Tree Index Implementation

*Part of: Component Documentation > Index Structures*

## Overview

The B+Tree index is a fundamental component of BayunDB that provides efficient key-based lookups, range scans, and data organization. This document describes the implementation details, algorithms, and usage patterns of the B+Tree index in BayunDB.

## Table of Contents

- [Core Design](#core-design)
- [Node Structure](#node-structure)
- [Key Operations](#key-operations)
- [Concurrency Considerations](#concurrency-considerations)
- [Performance Characteristics](#performance-characteristics)
- [Usage Examples](#usage-examples)
- [Future Enhancements](#future-enhancements)

## Core Design

BayunDB's B+Tree implementation follows the classic B+Tree structure with the following characteristics:

- Balanced tree structure with all leaf nodes at the same level
- Internal nodes store keys and child pointers
- Leaf nodes store keys and data pointers (record IDs)
- All keys are sorted to enable efficient range queries
- Leaf nodes are linked together to facilitate range scans

The implementation is designed to work efficiently with the buffer pool manager, ensuring that B+Tree nodes are properly cached and persisted. Each B+Tree node corresponds to a page in the buffer pool.

## Node Structure

### Internal Node Layout

Each internal node in the B+Tree contains:

- A header with metadata (node type, key count, etc.)
- An array of keys
- An array of child page IDs

```
+----------------+-------------------+-------------------+
| Header         | Keys              | Child Page IDs    |
|                | [k1, k2, ..., kn] | [p0, p1, ..., pn] |
+----------------+-------------------+-------------------+
```

### Leaf Node Layout

Each leaf node contains:

- A header with metadata (node type, key count, etc.)
- An array of keys
- An array of record IDs (RIDs)
- A pointer to the next leaf node (for range scans)

```
+----------------+-------------------+-------------------+------------+
| Header         | Keys              | Record IDs        | Next Leaf  |
|                | [k1, k2, ..., kn] | [r1, r2, ..., rn] | Page ID    |
+----------------+-------------------+-------------------+------------+
```

## Key Operations

### Insertion

The insertion algorithm follows these steps:

1. Traverse the tree from root to leaf to find the appropriate leaf node
2. Insert the key-value pair into the leaf node
3. If the leaf node is full, split it and propagate changes upward
4. Update parent nodes as necessary
5. If the root splits, create a new root

```rust
pub fn insert(&self, key: K, value: RecordId) -> Result<()> {
    // Implementation details...
}
```

### Search

The search algorithm:

1. Start at the root node
2. For internal nodes, perform binary search to find the appropriate child
3. Traverse down to the leaf level
4. In the leaf node, perform binary search to find the key
5. Return the associated record ID or indicate that the key was not found

```rust
pub fn search(&self, key: &K) -> Result<Option<RecordId>> {
    // Implementation details...
}
```

### Range Scan

Range scans leverage the linked list structure of leaf nodes:

1. Search for the first key in the range
2. Scan sequentially through the leaf node
3. When reaching the end of a leaf node, follow the next-leaf pointer
4. Continue until the end of the range or the end of the tree

```rust
pub fn range_scan(&self, start: &K, end: &K) -> Result<RangeScanIterator<K>> {
    // Implementation details...
}
```

### Deletion

The deletion algorithm:

1. Find the leaf node containing the key
2. Remove the key and associated record ID
3. If necessary, rebalance the tree through redistribution or merging
4. Update parent nodes as necessary
5. If the root has only one child, make that child the new root

```rust
pub fn delete(&self, key: &K) -> Result<bool> {
    // Implementation details...
}
```

## Concurrency Considerations

The current B+Tree implementation provides:

- Thread safety through the use of Rust's ownership model and `RwLock`
- Page-level locking via the buffer pool manager
- Atomic operations for tree modifications

Future enhancements will include:

- Fine-grained locking for higher concurrency
- Lock coupling techniques to reduce lock contention
- Support for multi-version concurrency control (MVCC)

## Performance Characteristics

### Time Complexity

| Operation | Average Case | Worst Case |
|-----------|--------------|------------|
| Search    | O(log n)     | O(log n)   |
| Insert    | O(log n)     | O(log n)   |
| Delete    | O(log n)     | O(log n)   |
| Range Scan| O(log n + m) | O(log n + m) |

Where n is the number of keys in the tree and m is the number of keys in the range.

### Space Efficiency

- Internal nodes have a fanout of approximately 100-200 keys per node (depending on key size)
- Leaf nodes store approximately 50-100 key-value pairs per node
- The tree height is typically 2-4 levels for databases with millions of records

### Benchmark Results

Based on our performance benchmarks:

- Insert operations: ~50,000 per second
- Point lookups: ~100,000 per second
- Range scans: ~10,000 keys per second

See the [benchmarking documentation](../development/benchmarking.md) for more detailed performance analysis.

## Usage Examples

### Basic Usage

```rust
// Create a B+Tree index for integer keys
let btree_index = BTreeIndex::<i32>::new(buffer_pool.clone())?;

// Insert a key-value pair
let record_id = RecordId::new(page_id, slot_id);
btree_index.insert(42, record_id)?;

// Look up a key
if let Some(rid) = btree_index.search(&42)? {
    println!("Found record: {}", rid);
}

// Perform a range scan
let mut scanner = btree_index.range_scan(&10, &50)?;
while let Some(entry) = scanner.next()? {
    println!("Key: {}, Value: {}", entry.key, entry.value);
}
```

### Integration with Query Execution

```rust
// Example of integrating with the query execution engine
let index_scan = IndexScan::new(
    btree_index.clone(),
    Bound::Included(10),
    Bound::Excluded(50),
);

let filter = Filter::new(
    Box::new(index_scan),
    |row| row.get::<i32>("id") % 2 == 0,
);

let mut executor = ExecutionEngine::new();
let result = executor.execute(Box::new(filter))?;
```

## Future Enhancements

Planned improvements to the B+Tree implementation:

1. **Support for compound keys**
   - Enable indexing on multiple columns
   - Variable-length key support

2. **Concurrency optimizations**
   - Lock-free operations where possible
   - Better handling of concurrent insert/delete operations

3. **Bulk loading**
   - Efficient construction for large initial datasets
   - Optimized algorithm for sorted input data

4. **Compression techniques**
   - Prefix compression for keys
   - Efficient storage of duplicate values 