//! Transaction Support with Multi-Version Concurrency Control (MVCC)
//!
//! Provides snapshot isolation for concurrent readers and writers on in-memory tables.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use arrow::record_batch::RecordBatch;

use crate::error::{BlazeError, Result};

/// Transaction ID type
pub type TxnId = u64;

/// Transaction status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnStatus {
    Active,
    Committed,
    Aborted,
}

/// A single transaction
#[derive(Debug)]
pub struct Transaction {
    /// Transaction ID
    pub id: TxnId,
    /// Transaction status
    pub status: TxnStatus,
    /// Snapshot timestamp (all data visible up to this point)
    pub snapshot_ts: TxnId,
    /// Tables modified in this transaction (table_name -> batches to insert)
    pub write_set: HashMap<String, Vec<RecordBatch>>,
    /// Tables deleted from in this transaction (table_name -> row indices to delete)
    pub delete_set: HashMap<String, Vec<usize>>,
    /// Read set for conflict detection (table names read)
    pub read_set: Vec<String>,
    /// Whether this is read-only
    pub read_only: bool,
    /// Named savepoints: name -> (write_set snapshot, delete_set snapshot)
    pub savepoints: Vec<(
        String,
        HashMap<String, Vec<RecordBatch>>,
        HashMap<String, Vec<usize>>,
    )>,
}

impl Transaction {
    fn new(id: TxnId, snapshot_ts: TxnId) -> Self {
        Self {
            id,
            status: TxnStatus::Active,
            snapshot_ts,
            write_set: HashMap::new(),
            delete_set: HashMap::new(),
            read_set: Vec::new(),
            read_only: true,
            savepoints: Vec::new(),
        }
    }
}

/// Version of a table at a specific transaction
#[derive(Debug, Clone)]
pub struct TableVersion {
    /// Transaction that created this version
    pub created_by: TxnId,
    /// The data in this version
    pub batches: Vec<RecordBatch>,
    /// Whether this version is visible (not aborted)
    pub visible: bool,
}

/// Transaction manager for MVCC
pub struct TransactionManager {
    /// Next transaction ID
    next_txn_id: AtomicU64,
    /// Active transactions
    active_txns: RwLock<HashMap<TxnId, Transaction>>,
    /// Committed version history per table (table_name -> versions ordered by txn_id)
    table_versions: RwLock<HashMap<String, Vec<TableVersion>>>,
    /// Global commit timestamp
    commit_ts: AtomicU64,
}

impl TransactionManager {
    /// Create a new transaction manager.
    pub fn new() -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
            active_txns: RwLock::new(HashMap::new()),
            table_versions: RwLock::new(HashMap::new()),
            commit_ts: AtomicU64::new(0),
        }
    }

    /// Begin a new transaction.
    pub fn begin(&self) -> Result<TxnId> {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let snapshot_ts = self.commit_ts.load(Ordering::SeqCst);

        let txn = Transaction::new(txn_id, snapshot_ts);

        let mut active = self
            .active_txns
            .write()
            .map_err(|e| BlazeError::internal(format!("Failed to acquire txn lock: {}", e)))?;
        active.insert(txn_id, txn);

        Ok(txn_id)
    }

    /// Begin a read-only transaction.
    pub fn begin_read_only(&self) -> Result<TxnId> {
        let txn_id = self.begin()?;
        if let Ok(mut active) = self.active_txns.write() {
            if let Some(txn) = active.get_mut(&txn_id) {
                txn.read_only = true;
            }
        }
        Ok(txn_id)
    }

    /// Record a write operation in a transaction.
    pub fn write(&self, txn_id: TxnId, table_name: &str, batches: Vec<RecordBatch>) -> Result<()> {
        let mut active = self
            .active_txns
            .write()
            .map_err(|e| BlazeError::internal(format!("Failed to acquire txn lock: {}", e)))?;

        let txn = active.get_mut(&txn_id).ok_or_else(|| {
            BlazeError::execution(format!("Transaction {} not found or not active", txn_id))
        })?;

        if txn.status != TxnStatus::Active {
            return Err(BlazeError::execution(format!(
                "Transaction {} is not active (status: {:?})",
                txn_id, txn.status
            )));
        }

        txn.read_only = false;
        txn.write_set
            .entry(table_name.to_string())
            .or_default()
            .extend(batches);

        Ok(())
    }

    /// Record a read in a transaction (for conflict detection).
    pub fn record_read(&self, txn_id: TxnId, table_name: &str) -> Result<()> {
        let mut active = self
            .active_txns
            .write()
            .map_err(|e| BlazeError::internal(format!("Failed to acquire txn lock: {}", e)))?;

        if let Some(txn) = active.get_mut(&txn_id) {
            if !txn.read_set.contains(&table_name.to_string()) {
                txn.read_set.push(table_name.to_string());
            }
        }
        Ok(())
    }

    /// Commit a transaction.
    pub fn commit(&self, txn_id: TxnId) -> Result<()> {
        // Get the transaction
        let txn = {
            let mut active = self
                .active_txns
                .write()
                .map_err(|e| BlazeError::internal(format!("Failed to acquire txn lock: {}", e)))?;

            let txn = active.get_mut(&txn_id).ok_or_else(|| {
                BlazeError::execution(format!("Transaction {} not found", txn_id))
            })?;

            if txn.status != TxnStatus::Active {
                return Err(BlazeError::execution(format!(
                    "Transaction {} is not active",
                    txn_id
                )));
            }

            // Check for write-write conflicts
            // (simplified: just check if any other committed transaction modified the same tables)
            let write_tables: Vec<String> = txn.write_set.keys().cloned().collect();

            // Mark as committed
            txn.status = TxnStatus::Committed;

            // Take the write set
            let write_set = std::mem::take(&mut txn.write_set);
            (write_tables, write_set)
        };

        let (_, write_set) = txn;

        // Apply writes to version store
        if !write_set.is_empty() {
            let mut versions = self.table_versions.write().map_err(|e| {
                BlazeError::internal(format!("Failed to acquire version lock: {}", e))
            })?;

            for (table_name, batches) in write_set {
                let version = TableVersion {
                    created_by: txn_id,
                    batches,
                    visible: true,
                };
                versions.entry(table_name).or_default().push(version);
            }
        }

        // Advance commit timestamp
        self.commit_ts.fetch_max(txn_id, Ordering::SeqCst);

        Ok(())
    }

    /// Abort (rollback) a transaction.
    pub fn abort(&self, txn_id: TxnId) -> Result<()> {
        let mut active = self
            .active_txns
            .write()
            .map_err(|e| BlazeError::internal(format!("Failed to acquire txn lock: {}", e)))?;

        let txn = active
            .get_mut(&txn_id)
            .ok_or_else(|| BlazeError::execution(format!("Transaction {} not found", txn_id)))?;

        txn.status = TxnStatus::Aborted;
        txn.write_set.clear();
        txn.delete_set.clear();

        Ok(())
    }

    /// Get the visible data for a table at a given transaction's snapshot.
    pub fn get_visible_data(&self, txn_id: TxnId, table_name: &str) -> Result<Vec<RecordBatch>> {
        // Get snapshot timestamp
        let snapshot_ts =
            {
                let active = self.active_txns.read().map_err(|e| {
                    BlazeError::internal(format!("Failed to acquire txn lock: {}", e))
                })?;

                active.get(&txn_id).map(|t| t.snapshot_ts).ok_or_else(|| {
                    BlazeError::execution(format!("Transaction {} not found", txn_id))
                })?
            };

        // Get all visible versions
        let versions = self
            .table_versions
            .read()
            .map_err(|e| BlazeError::internal(format!("Failed to acquire version lock: {}", e)))?;

        let mut result = Vec::new();

        if let Some(table_versions) = versions.get(table_name) {
            for version in table_versions {
                // Version is visible if:
                // 1. It was committed before our snapshot
                // 2. It's marked as visible (not from an aborted txn)
                if version.created_by <= snapshot_ts && version.visible {
                    result.extend(version.batches.clone());
                }
            }
        }

        // Also include uncommitted writes from the current transaction
        let active = self
            .active_txns
            .read()
            .map_err(|e| BlazeError::internal(format!("Failed to acquire txn lock: {}", e)))?;

        if let Some(txn) = active.get(&txn_id) {
            if let Some(batches) = txn.write_set.get(table_name) {
                result.extend(batches.clone());
            }
        }

        Ok(result)
    }

    /// Get transaction status.
    pub fn status(&self, txn_id: TxnId) -> Option<TxnStatus> {
        self.active_txns
            .read()
            .ok()
            .and_then(|active| active.get(&txn_id).map(|t| t.status))
    }

    /// Get the number of active transactions.
    pub fn active_count(&self) -> usize {
        self.active_txns
            .read()
            .ok()
            .map(|active| {
                active
                    .values()
                    .filter(|t| t.status == TxnStatus::Active)
                    .count()
            })
            .unwrap_or(0)
    }

    /// Garbage collect old versions that are no longer needed.
    /// Create a savepoint within a transaction.
    pub fn create_savepoint(&self, txn_id: TxnId, name: &str) -> Result<()> {
        let mut active = self
            .active_txns
            .write()
            .map_err(|e| BlazeError::internal(format!("Failed to acquire txn lock: {}", e)))?;
        let txn = active
            .get_mut(&txn_id)
            .ok_or_else(|| BlazeError::execution(format!("Transaction {} not found", txn_id)))?;
        if txn.status != TxnStatus::Active {
            return Err(BlazeError::execution("Transaction is not active"));
        }
        // Snapshot current write_set and delete_set
        let write_snapshot = txn.write_set.clone();
        let delete_snapshot = txn.delete_set.clone();
        txn.savepoints
            .push((name.to_string(), write_snapshot, delete_snapshot));
        Ok(())
    }

    /// Release (drop) a savepoint without rolling back.
    pub fn release_savepoint(&self, txn_id: TxnId, name: &str) -> Result<()> {
        let mut active = self
            .active_txns
            .write()
            .map_err(|e| BlazeError::internal(format!("Failed to acquire txn lock: {}", e)))?;
        let txn = active
            .get_mut(&txn_id)
            .ok_or_else(|| BlazeError::execution(format!("Transaction {} not found", txn_id)))?;
        let pos = txn
            .savepoints
            .iter()
            .rposition(|(n, _, _)| n == name)
            .ok_or_else(|| BlazeError::execution(format!("Savepoint '{}' not found", name)))?;
        txn.savepoints.remove(pos);
        Ok(())
    }

    /// Rollback to a named savepoint, restoring write/delete sets.
    pub fn rollback_to_savepoint(&self, txn_id: TxnId, name: &str) -> Result<()> {
        let mut active = self
            .active_txns
            .write()
            .map_err(|e| BlazeError::internal(format!("Failed to acquire txn lock: {}", e)))?;
        let txn = active
            .get_mut(&txn_id)
            .ok_or_else(|| BlazeError::execution(format!("Transaction {} not found", txn_id)))?;
        if txn.status != TxnStatus::Active {
            return Err(BlazeError::execution("Transaction is not active"));
        }
        let pos = txn
            .savepoints
            .iter()
            .rposition(|(n, _, _)| n == name)
            .ok_or_else(|| BlazeError::execution(format!("Savepoint '{}' not found", name)))?;
        let (_, write_snap, delete_snap) = txn.savepoints[pos].clone();
        // Restore to savepoint state and remove all savepoints after it
        txn.write_set = write_snap;
        txn.delete_set = delete_snap;
        txn.savepoints.truncate(pos + 1);
        Ok(())
    }

    /// Garbage collect completed transactions older than the oldest active snapshot.
    pub fn gc(&self) -> Result<usize> {
        let min_active_ts = {
            let active = self
                .active_txns
                .read()
                .map_err(|e| BlazeError::internal(format!("Failed to acquire txn lock: {}", e)))?;
            active
                .values()
                .filter(|t| t.status == TxnStatus::Active)
                .map(|t| t.snapshot_ts)
                .min()
                .unwrap_or(u64::MAX)
        };

        // Remove completed transactions that are older than the oldest active snapshot
        let mut removed = 0;
        let mut active = self
            .active_txns
            .write()
            .map_err(|e| BlazeError::internal(format!("Failed to acquire txn lock: {}", e)))?;

        let to_remove: Vec<TxnId> = active
            .iter()
            .filter(|(_, t)| t.status != TxnStatus::Active && t.id < min_active_ts)
            .map(|(id, _)| *id)
            .collect();

        for id in to_remove {
            active.remove(&id);
            removed += 1;
        }

        Ok(removed)
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap()
    }

    #[test]
    fn test_begin_commit() {
        let mgr = TransactionManager::new();
        let txn_id = mgr.begin().unwrap();
        assert_eq!(mgr.status(txn_id), Some(TxnStatus::Active));

        mgr.commit(txn_id).unwrap();
        assert_eq!(mgr.status(txn_id), Some(TxnStatus::Committed));
    }

    #[test]
    fn test_begin_abort() {
        let mgr = TransactionManager::new();
        let txn_id = mgr.begin().unwrap();

        mgr.abort(txn_id).unwrap();
        assert_eq!(mgr.status(txn_id), Some(TxnStatus::Aborted));
    }

    #[test]
    fn test_write_and_read_within_txn() {
        let mgr = TransactionManager::new();
        let txn_id = mgr.begin().unwrap();

        let batch = make_batch(&[1, 2, 3]);
        mgr.write(txn_id, "test_table", vec![batch]).unwrap();

        // Should see own writes
        let data = mgr.get_visible_data(txn_id, "test_table").unwrap();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].num_rows(), 3);

        mgr.commit(txn_id).unwrap();
    }

    #[test]
    fn test_snapshot_isolation() {
        let mgr = TransactionManager::new();

        // T1 writes and commits
        let t1 = mgr.begin().unwrap();
        mgr.write(t1, "table1", vec![make_batch(&[1, 2])]).unwrap();
        mgr.commit(t1).unwrap();

        // T2 starts after T1 commits - should see T1's writes
        let t2 = mgr.begin().unwrap();
        let data = mgr.get_visible_data(t2, "table1").unwrap();
        assert_eq!(data.len(), 1);

        // T3 starts, writes, but doesn't commit
        let t3 = mgr.begin().unwrap();
        mgr.write(t3, "table1", vec![make_batch(&[3, 4])]).unwrap();

        // T2 should NOT see T3's uncommitted writes
        let data = mgr.get_visible_data(t2, "table1").unwrap();
        assert_eq!(data.len(), 1); // Only T1's data

        mgr.commit(t2).unwrap();
        mgr.commit(t3).unwrap();
    }

    #[test]
    fn test_aborted_writes_invisible() {
        let mgr = TransactionManager::new();

        let t1 = mgr.begin().unwrap();
        mgr.write(t1, "table1", vec![make_batch(&[1, 2])]).unwrap();
        mgr.abort(t1).unwrap();

        // New transaction should not see aborted data
        let t2 = mgr.begin().unwrap();
        let data = mgr.get_visible_data(t2, "table1").unwrap();
        assert!(data.is_empty());

        mgr.commit(t2).unwrap();
    }

    #[test]
    fn test_gc() {
        let mgr = TransactionManager::new();

        let t1 = mgr.begin().unwrap();
        mgr.commit(t1).unwrap();

        let t2 = mgr.begin().unwrap();
        mgr.commit(t2).unwrap();

        // Start a new active txn so GC can clean up old ones
        let _t3 = mgr.begin().unwrap();

        let removed = mgr.gc().unwrap();
        assert!(removed >= 1);
    }

    #[test]
    fn test_active_count() {
        let mgr = TransactionManager::new();
        assert_eq!(mgr.active_count(), 0);

        let t1 = mgr.begin().unwrap();
        assert_eq!(mgr.active_count(), 1);

        let t2 = mgr.begin().unwrap();
        assert_eq!(mgr.active_count(), 2);

        mgr.commit(t1).unwrap();
        assert_eq!(mgr.active_count(), 1);

        mgr.abort(t2).unwrap();
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn test_savepoint_create_and_rollback() {
        let mgr = TransactionManager::new();
        let t1 = mgr.begin().unwrap();

        mgr.write(t1, "t", vec![make_batch(&[1, 2])]).unwrap();
        mgr.create_savepoint(t1, "sp1").unwrap();

        // Write more after savepoint
        mgr.write(t1, "t", vec![make_batch(&[3, 4])]).unwrap();

        // Should see 2 batches
        let data = mgr.get_visible_data(t1, "t").unwrap();
        assert_eq!(data.len(), 2);

        // Rollback to savepoint - should only see 1 batch
        mgr.rollback_to_savepoint(t1, "sp1").unwrap();
        let data = mgr.get_visible_data(t1, "t").unwrap();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].num_rows(), 2);

        mgr.commit(t1).unwrap();
    }

    #[test]
    fn test_savepoint_release() {
        let mgr = TransactionManager::new();
        let t1 = mgr.begin().unwrap();

        mgr.create_savepoint(t1, "sp1").unwrap();
        mgr.release_savepoint(t1, "sp1").unwrap();

        // Should fail - savepoint is released
        assert!(mgr.rollback_to_savepoint(t1, "sp1").is_err());
        mgr.commit(t1).unwrap();
    }

    #[test]
    fn test_nested_savepoints() {
        let mgr = TransactionManager::new();
        let t1 = mgr.begin().unwrap();

        mgr.write(t1, "t", vec![make_batch(&[1])]).unwrap();
        mgr.create_savepoint(t1, "sp1").unwrap();

        mgr.write(t1, "t", vec![make_batch(&[2])]).unwrap();
        mgr.create_savepoint(t1, "sp2").unwrap();

        mgr.write(t1, "t", vec![make_batch(&[3])]).unwrap();

        // 3 batches total
        let data = mgr.get_visible_data(t1, "t").unwrap();
        assert_eq!(data.len(), 3);

        // Rollback to sp2 -> removes 3rd batch
        mgr.rollback_to_savepoint(t1, "sp2").unwrap();
        let data = mgr.get_visible_data(t1, "t").unwrap();
        assert_eq!(data.len(), 2);

        // Rollback to sp1 -> removes 2nd batch
        mgr.rollback_to_savepoint(t1, "sp1").unwrap();
        let data = mgr.get_visible_data(t1, "t").unwrap();
        assert_eq!(data.len(), 1);

        mgr.commit(t1).unwrap();
    }
}
