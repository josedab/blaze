//! Exchange Operators for Data Redistribution
//!
//! Exchange operators handle data movement between parallel workers,
//! implementing shuffle and broadcast operations.

use std::sync::Mutex;

use arrow::record_batch::RecordBatch;

use super::partition::{HashPartitioner, RoundRobinPartitioner};
use crate::error::{BlazeError, Result};

/// Type of exchange operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExchangeType {
    /// Hash-based shuffle to redistribute data
    Shuffle,
    /// Broadcast data to all partitions
    Broadcast,
    /// Gather all data to a single partition
    Gather,
    /// Round-robin distribution
    RoundRobin,
    /// Repartition to change partition count
    Repartition,
}

/// Exchange operator trait for data redistribution.
pub trait Exchange: Send + Sync {
    /// Get the exchange type.
    fn exchange_type(&self) -> ExchangeType;

    /// Get the number of input partitions.
    fn input_partitions(&self) -> usize;

    /// Get the number of output partitions.
    fn output_partitions(&self) -> usize;

    /// Execute the exchange on input batches.
    fn execute(&self, inputs: Vec<Vec<RecordBatch>>) -> Result<Vec<Vec<RecordBatch>>>;
}

/// Shuffle exchange redistributes data based on hash partitioning.
#[derive(Debug)]
pub struct ShuffleExchange {
    input_partitions: usize,
    output_partitions: usize,
    key_columns: Vec<usize>,
}

impl ShuffleExchange {
    /// Create a new shuffle exchange.
    pub fn new(input_partitions: usize, output_partitions: usize, key_columns: Vec<usize>) -> Self {
        Self {
            input_partitions,
            output_partitions,
            key_columns,
        }
    }
}

impl Exchange for ShuffleExchange {
    fn exchange_type(&self) -> ExchangeType {
        ExchangeType::Shuffle
    }

    fn input_partitions(&self) -> usize {
        self.input_partitions
    }

    fn output_partitions(&self) -> usize {
        self.output_partitions
    }

    fn execute(&self, inputs: Vec<Vec<RecordBatch>>) -> Result<Vec<Vec<RecordBatch>>> {
        let partitioner = HashPartitioner::new(self.output_partitions, self.key_columns.clone());

        // Partition each input batch
        let mut output_buffers: Vec<Vec<RecordBatch>> = vec![Vec::new(); self.output_partitions];

        for input_batches in inputs {
            for batch in input_batches {
                if batch.num_rows() == 0 {
                    continue;
                }

                let partitioned = partitioner.partition(&batch)?;
                for (i, partition_batch) in partitioned.into_iter().enumerate() {
                    if partition_batch.num_rows() > 0 {
                        output_buffers[i].push(partition_batch);
                    }
                }
            }
        }

        Ok(output_buffers)
    }
}

/// Broadcast exchange sends data to all output partitions.
#[derive(Debug)]
pub struct BroadcastExchange {
    input_partitions: usize,
    output_partitions: usize,
}

impl BroadcastExchange {
    /// Create a new broadcast exchange.
    pub fn new(input_partitions: usize, output_partitions: usize) -> Self {
        Self {
            input_partitions,
            output_partitions,
        }
    }
}

impl Exchange for BroadcastExchange {
    fn exchange_type(&self) -> ExchangeType {
        ExchangeType::Broadcast
    }

    fn input_partitions(&self) -> usize {
        self.input_partitions
    }

    fn output_partitions(&self) -> usize {
        self.output_partitions
    }

    fn execute(&self, inputs: Vec<Vec<RecordBatch>>) -> Result<Vec<Vec<RecordBatch>>> {
        // Collect all input batches
        let all_batches: Vec<RecordBatch> = inputs.into_iter().flatten().collect();

        // Broadcast to all output partitions
        let output: Vec<Vec<RecordBatch>> = (0..self.output_partitions)
            .map(|_| all_batches.clone())
            .collect();

        Ok(output)
    }
}

/// Gather exchange collects all data into a single partition.
#[derive(Debug)]
#[allow(dead_code)]
pub struct GatherExchange {
    input_partitions: usize,
}

#[allow(dead_code)]
impl GatherExchange {
    /// Create a new gather exchange.
    pub fn new(input_partitions: usize) -> Self {
        Self { input_partitions }
    }
}

impl Exchange for GatherExchange {
    fn exchange_type(&self) -> ExchangeType {
        ExchangeType::Gather
    }

    fn input_partitions(&self) -> usize {
        self.input_partitions
    }

    fn output_partitions(&self) -> usize {
        1
    }

    fn execute(&self, inputs: Vec<Vec<RecordBatch>>) -> Result<Vec<Vec<RecordBatch>>> {
        // Collect all batches into a single partition
        let all_batches: Vec<RecordBatch> = inputs.into_iter().flatten().collect();
        Ok(vec![all_batches])
    }
}

/// Round-robin exchange distributes data evenly.
#[derive(Debug)]
#[allow(dead_code)]
pub struct RoundRobinExchange {
    input_partitions: usize,
    output_partitions: usize,
}

#[allow(dead_code)]
impl RoundRobinExchange {
    /// Create a new round-robin exchange.
    pub fn new(input_partitions: usize, output_partitions: usize) -> Self {
        Self {
            input_partitions,
            output_partitions,
        }
    }
}

impl Exchange for RoundRobinExchange {
    fn exchange_type(&self) -> ExchangeType {
        ExchangeType::RoundRobin
    }

    fn input_partitions(&self) -> usize {
        self.input_partitions
    }

    fn output_partitions(&self) -> usize {
        self.output_partitions
    }

    fn execute(&self, inputs: Vec<Vec<RecordBatch>>) -> Result<Vec<Vec<RecordBatch>>> {
        let partitioner = RoundRobinPartitioner::new(self.output_partitions);

        let mut output_buffers: Vec<Vec<RecordBatch>> = vec![Vec::new(); self.output_partitions];

        for input_batches in inputs {
            for batch in input_batches {
                if batch.num_rows() == 0 {
                    continue;
                }

                let partitioned = partitioner.partition(&batch)?;
                for (i, partition_batch) in partitioned.into_iter().enumerate() {
                    if partition_batch.num_rows() > 0 {
                        output_buffers[i].push(partition_batch);
                    }
                }
            }
        }

        Ok(output_buffers)
    }
}

/// Exchange buffer for collecting data during parallel execution.
#[derive(Debug)]
#[allow(dead_code)]
pub struct ExchangeBuffer {
    buffers: Vec<Mutex<Vec<RecordBatch>>>,
    num_partitions: usize,
}

#[allow(dead_code)]
impl ExchangeBuffer {
    /// Create a new exchange buffer.
    pub fn new(num_partitions: usize) -> Self {
        let buffers = (0..num_partitions)
            .map(|_| Mutex::new(Vec::new()))
            .collect();

        Self {
            buffers,
            num_partitions,
        }
    }

    /// Push a batch to a specific partition.
    pub fn push(&self, partition_id: usize, batch: RecordBatch) -> Result<()> {
        if partition_id >= self.num_partitions {
            return Err(BlazeError::execution(format!(
                "Invalid partition id: {} (max: {})",
                partition_id,
                self.num_partitions - 1
            )));
        }

        let mut buffer = self.buffers[partition_id]
            .lock()
            .map_err(|_| BlazeError::execution("Failed to acquire buffer lock"))?;
        buffer.push(batch);
        Ok(())
    }

    /// Take all batches from a partition.
    pub fn take(&self, partition_id: usize) -> Result<Vec<RecordBatch>> {
        if partition_id >= self.num_partitions {
            return Err(BlazeError::execution(format!(
                "Invalid partition id: {} (max: {})",
                partition_id,
                self.num_partitions - 1
            )));
        }

        let mut buffer = self.buffers[partition_id]
            .lock()
            .map_err(|_| BlazeError::execution("Failed to acquire buffer lock"))?;
        Ok(std::mem::take(&mut *buffer))
    }

    /// Get the number of partitions.
    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    /// Take all batches from all partitions.
    pub fn take_all(&self) -> Result<Vec<Vec<RecordBatch>>> {
        let mut result = Vec::with_capacity(self.num_partitions);
        for i in 0..self.num_partitions {
            result.push(self.take(i)?);
        }
        Ok(result)
    }
}

/// Builder for creating exchange operators.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ExchangeBuilder {
    input_partitions: usize,
    output_partitions: usize,
    exchange_type: ExchangeType,
    key_columns: Vec<usize>,
}

#[allow(dead_code)]
impl ExchangeBuilder {
    /// Create a new exchange builder.
    pub fn new(input_partitions: usize, output_partitions: usize) -> Self {
        Self {
            input_partitions,
            output_partitions,
            exchange_type: ExchangeType::RoundRobin,
            key_columns: Vec::new(),
        }
    }

    /// Set the exchange type to shuffle.
    pub fn shuffle(mut self, key_columns: Vec<usize>) -> Self {
        self.exchange_type = ExchangeType::Shuffle;
        self.key_columns = key_columns;
        self
    }

    /// Set the exchange type to broadcast.
    pub fn broadcast(mut self) -> Self {
        self.exchange_type = ExchangeType::Broadcast;
        self
    }

    /// Set the exchange type to gather.
    pub fn gather(mut self) -> Self {
        self.exchange_type = ExchangeType::Gather;
        self.output_partitions = 1;
        self
    }

    /// Set the exchange type to round-robin.
    pub fn round_robin(mut self) -> Self {
        self.exchange_type = ExchangeType::RoundRobin;
        self
    }

    /// Build the exchange operator.
    pub fn build(self) -> Box<dyn Exchange> {
        match self.exchange_type {
            ExchangeType::Shuffle => Box::new(ShuffleExchange::new(
                self.input_partitions,
                self.output_partitions,
                self.key_columns,
            )),
            ExchangeType::Broadcast => Box::new(BroadcastExchange::new(
                self.input_partitions,
                self.output_partitions,
            )),
            ExchangeType::Gather => Box::new(GatherExchange::new(self.input_partitions)),
            ExchangeType::RoundRobin | ExchangeType::Repartition => Box::new(
                RoundRobinExchange::new(self.input_partitions, self.output_partitions),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_test_batch(ids: &[i64], names: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int64Array::from(ids.to_vec());
        let name_array = StringArray::from(names.to_vec());

        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap()
    }

    #[test]
    fn test_shuffle_exchange() {
        let batch1 = make_test_batch(&[1, 2, 3, 4], &["a", "b", "c", "d"]);
        let batch2 = make_test_batch(&[5, 6, 7, 8], &["e", "f", "g", "h"]);

        let exchange = ShuffleExchange::new(2, 4, vec![0]);
        let result = exchange.execute(vec![vec![batch1], vec![batch2]]).unwrap();

        assert_eq!(result.len(), 4);

        let total_rows: usize = result
            .iter()
            .flat_map(|batches| batches.iter())
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(total_rows, 8);
    }

    #[test]
    fn test_broadcast_exchange() {
        let batch = make_test_batch(&[1, 2, 3], &["a", "b", "c"]);

        let exchange = BroadcastExchange::new(1, 4);
        let result = exchange.execute(vec![vec![batch]]).unwrap();

        assert_eq!(result.len(), 4);

        // Each partition should have the same data
        for partition in &result {
            assert_eq!(partition.len(), 1);
            assert_eq!(partition[0].num_rows(), 3);
        }
    }

    #[test]
    fn test_gather_exchange() {
        let batch1 = make_test_batch(&[1, 2], &["a", "b"]);
        let batch2 = make_test_batch(&[3, 4], &["c", "d"]);

        let exchange = GatherExchange::new(2);
        let result = exchange.execute(vec![vec![batch1], vec![batch2]]).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 2);

        let total_rows: usize = result[0].iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4);
    }

    #[test]
    fn test_exchange_buffer() {
        let buffer = ExchangeBuffer::new(4);

        let batch = make_test_batch(&[1, 2], &["a", "b"]);
        buffer.push(0, batch.clone()).unwrap();
        buffer.push(2, batch.clone()).unwrap();

        let partition0 = buffer.take(0).unwrap();
        assert_eq!(partition0.len(), 1);
        assert_eq!(partition0[0].num_rows(), 2);

        let partition1 = buffer.take(1).unwrap();
        assert!(partition1.is_empty());
    }

    #[test]
    fn test_exchange_builder() {
        let exchange = ExchangeBuilder::new(4, 8).shuffle(vec![0]).build();

        assert_eq!(exchange.exchange_type(), ExchangeType::Shuffle);
        assert_eq!(exchange.input_partitions(), 4);
        assert_eq!(exchange.output_partitions(), 8);
    }
}
