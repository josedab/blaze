//! Vector similarity search for Blaze.
//!
//! Provides vector data types, distance functions, and k-NN search
//! capabilities for semantic search and embedding-based analytics.

use std::collections::{BinaryHeap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float32Array};

use crate::error::{BlazeError, Result};

/// Distance metric for vector similarity search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DistanceMetric {
    /// Euclidean (L2) distance.
    L2,
    /// Cosine distance (1 - cosine similarity).
    Cosine,
    /// Dot product (negative, for use as a distance).
    DotProduct,
    /// Inner product (alias for dot product).
    InnerProduct,
}

// ---------------------------------------------------------------------------
// Scalar distance functions
// ---------------------------------------------------------------------------

/// Compute the L2 (Euclidean) distance between two vectors.
///
/// Returns an error if the vectors have different lengths.
pub fn l2_distance(a: &[f32], b: &[f32]) -> Result<f32> {
    if a.len() != b.len() {
        return Err(BlazeError::invalid_argument(format!(
            "Vector dimension mismatch: {} vs {}",
            a.len(),
            b.len()
        )));
    }
    let sum: f32 = a.iter().zip(b.iter()).map(|(x, y)| (x - y).powi(2)).sum();
    Ok(sum.sqrt())
}

/// Compute the cosine distance between two vectors (1 - cosine_similarity).
///
/// Returns an error if the vectors have different lengths.
pub fn cosine_distance(a: &[f32], b: &[f32]) -> Result<f32> {
    Ok(1.0 - cosine_similarity(a, b)?)
}

/// Compute the dot product of two vectors.
///
/// Returns an error if the vectors have different lengths.
pub fn dot_product(a: &[f32], b: &[f32]) -> Result<f32> {
    if a.len() != b.len() {
        return Err(BlazeError::invalid_argument(format!(
            "Vector dimension mismatch: {} vs {}",
            a.len(),
            b.len()
        )));
    }
    Ok(a.iter().zip(b.iter()).map(|(x, y)| x * y).sum())
}

/// Compute the cosine similarity between two vectors.
///
/// Returns 0.0 when either vector has zero magnitude.
/// Returns an error if the vectors have different lengths.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> Result<f32> {
    if a.len() != b.len() {
        return Err(BlazeError::invalid_argument(format!(
            "Vector dimension mismatch: {} vs {}",
            a.len(),
            b.len()
        )));
    }
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();
    let denom = norm_a * norm_b;
    if denom == 0.0 {
        return Ok(0.0);
    }
    Ok(dot / denom)
}

// ---------------------------------------------------------------------------
// VectorIndex trait & BruteForceIndex
// ---------------------------------------------------------------------------

/// Trait for vector similarity search indices.
pub trait VectorIndex: Send + Sync {
    /// Search for the k nearest neighbors to the given query vector.
    ///
    /// Returns a list of `(row_index, distance)` pairs sorted by distance
    /// (ascending).
    fn search_knn(&self, query: &[f32], k: usize) -> Result<Vec<(usize, f32)>>;
}

/// Brute-force (flat) vector index – scans all vectors on every query.
#[derive(Debug, Clone)]
pub struct BruteForceIndex {
    vectors: Vec<Vec<f32>>,
    metric: DistanceMetric,
}

impl BruteForceIndex {
    /// Create a new brute-force index with the given distance metric.
    pub fn new(metric: DistanceMetric) -> Self {
        Self {
            vectors: Vec::new(),
            metric,
        }
    }

    /// Add a vector to the index.
    pub fn add(&mut self, vector: Vec<f32>) {
        self.vectors.push(vector);
    }

    /// Return the number of vectors in the index.
    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    /// Return true if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }

    /// Compute the distance between two vectors using this index's metric.
    fn distance(&self, a: &[f32], b: &[f32]) -> Result<f32> {
        match self.metric {
            DistanceMetric::L2 => l2_distance(a, b),
            DistanceMetric::Cosine => cosine_distance(a, b),
            DistanceMetric::DotProduct | DistanceMetric::InnerProduct => {
                // Negate so that larger dot products yield smaller "distance".
                Ok(-dot_product(a, b)?)
            }
        }
    }
}

impl VectorIndex for BruteForceIndex {
    fn search_knn(&self, query: &[f32], k: usize) -> Result<Vec<(usize, f32)>> {
        let mut distances: Vec<(usize, f32)> = self
            .vectors
            .iter()
            .enumerate()
            .map(|(i, v)| self.distance(query, v).map(|d| (i, d)))
            .collect::<Result<Vec<_>>>()?;

        distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        distances.truncate(k);
        Ok(distances)
    }
}

// ---------------------------------------------------------------------------
// VectorOps – Arrow-level helpers
// ---------------------------------------------------------------------------

/// Arrow-level vector operations.
pub struct VectorOps;

impl VectorOps {
    /// Compute distances from every vector in `vectors` to the given `query`.
    ///
    /// `vectors` must be a `FixedSizeList(Float32, dim)` array.
    pub fn compute_distances(
        vectors: &ArrayRef,
        query: &[f32],
        metric: DistanceMetric,
    ) -> Result<Float32Array> {
        let fsl = vectors
            .as_fixed_size_list_opt()
            .ok_or_else(|| BlazeError::type_error("Expected FixedSizeList array for vectors"))?;

        let values = fsl
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| BlazeError::type_error("Expected Float32 values inside vector array"))?;

        let dim = fsl.value_length() as usize;
        if query.len() != dim {
            return Err(BlazeError::invalid_argument(format!(
                "Query dimension {} does not match vector dimension {}",
                query.len(),
                dim
            )));
        }

        let num_rows = fsl.len();
        let mut result = Vec::with_capacity(num_rows);

        for row in 0..num_rows {
            if fsl.is_null(row) {
                result.push(f32::NAN);
                continue;
            }
            let offset = row * dim;
            let vec_slice: Vec<f32> = (0..dim).map(|j| values.value(offset + j)).collect();

            let dist = match metric {
                DistanceMetric::L2 => l2_distance(&vec_slice, query)?,
                DistanceMetric::Cosine => cosine_distance(&vec_slice, query)?,
                DistanceMetric::DotProduct | DistanceMetric::InnerProduct => {
                    -dot_product(&vec_slice, query)?
                }
            };
            result.push(dist);
        }

        Ok(Float32Array::from(result))
    }

    /// Return the indices and distances of the `k` smallest distances.
    ///
    /// Null / NaN distances are excluded. The result is sorted ascending by
    /// distance.
    pub fn top_k(distances: &Float32Array, k: usize) -> Result<Vec<(usize, f32)>> {
        let mut indexed: Vec<(usize, f32)> = distances
            .iter()
            .enumerate()
            .filter_map(|(i, v)| v.filter(|d| !d.is_nan()).map(|d| (i, d)))
            .collect();

        indexed.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        indexed.truncate(k);
        Ok(indexed)
    }

    /// Build a `FixedSizeList(Float32, dim)` Arrow array from a slice of vectors.
    pub fn build_vector_array(vectors: &[Vec<f32>], dimension: usize) -> Result<ArrayRef> {
        use arrow::array::FixedSizeListArray;
        use arrow::datatypes::{DataType as ArrowDataType, Field};

        let flat: Vec<f32> = vectors.iter().flat_map(|v| v.iter().copied()).collect();
        let values = Float32Array::from(flat);
        let field = Arc::new(Field::new("item", ArrowDataType::Float32, true));
        let arr = FixedSizeListArray::new(field, dimension as i32, Arc::new(values), None);
        Ok(Arc::new(arr))
    }
}

// ---------------------------------------------------------------------------
// SIMD-optimized distance functions
// ---------------------------------------------------------------------------

/// SIMD-optimized L2 distance computation.
/// Falls back to scalar on unsupported architectures.
pub fn l2_distance_simd(a: &[f32], b: &[f32]) -> Result<f32> {
    if a.len() != b.len() {
        return Err(BlazeError::invalid_argument(format!(
            "Vector dimension mismatch: {} vs {}",
            a.len(),
            b.len()
        )));
    }
    // Process in chunks of 8 for manual SIMD-style computation
    let chunks = a.len() / 8;
    let mut sum = 0.0f32;

    for chunk in 0..chunks {
        let base = chunk * 8;
        let mut local_sum = 0.0f32;
        for i in 0..8 {
            let diff = a[base + i] - b[base + i];
            local_sum += diff * diff;
        }
        sum += local_sum;
    }

    // Handle remaining elements
    for i in (chunks * 8)..a.len() {
        let diff = a[i] - b[i];
        sum += diff * diff;
    }

    Ok(sum.sqrt())
}

/// SIMD-optimized dot product computation.
pub fn dot_product_simd(a: &[f32], b: &[f32]) -> Result<f32> {
    if a.len() != b.len() {
        return Err(BlazeError::invalid_argument(format!(
            "Vector dimension mismatch: {} vs {}",
            a.len(),
            b.len()
        )));
    }
    let chunks = a.len() / 8;
    let mut sum = 0.0f32;

    for chunk in 0..chunks {
        let base = chunk * 8;
        let mut local_sum = 0.0f32;
        for i in 0..8 {
            local_sum += a[base + i] * b[base + i];
        }
        sum += local_sum;
    }

    for i in (chunks * 8)..a.len() {
        sum += a[i] * b[i];
    }

    Ok(sum)
}

/// Batch distance computation using SIMD-optimized functions.
pub fn batch_distances_simd(
    query: &[f32],
    vectors: &[Vec<f32>],
    metric: DistanceMetric,
) -> Result<Vec<f32>> {
    vectors
        .iter()
        .map(|v| match metric {
            DistanceMetric::L2 => l2_distance_simd(query, v),
            DistanceMetric::Cosine => cosine_distance(query, v),
            DistanceMetric::DotProduct | DistanceMetric::InnerProduct => {
                Ok(-dot_product_simd(query, v)?)
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// ORDER BY k-NN integration
// ---------------------------------------------------------------------------

/// Result of a k-NN search with row indices and distances.
#[derive(Debug, Clone)]
pub struct KnnResult {
    /// Row indices sorted by distance (ascending).
    pub indices: Vec<usize>,
    /// Corresponding distances.
    pub distances: Vec<f32>,
}

/// Performs k-NN search on an Arrow FixedSizeList column.
/// Returns indices and distances for use with ORDER BY integration.
pub struct VectorKnnSearch;

impl VectorKnnSearch {
    /// Execute a k-NN search against a vector column.
    pub fn search(
        vectors: &ArrayRef,
        query: &[f32],
        k: usize,
        metric: DistanceMetric,
    ) -> Result<KnnResult> {
        let dists = VectorOps::compute_distances(vectors, query, metric)?;
        let mut top_k = VectorOps::top_k(&dists, k)?;
        top_k.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let indices = top_k.iter().map(|(i, _)| *i).collect();
        let distances = top_k.iter().map(|(_, d)| *d).collect();

        Ok(KnnResult { indices, distances })
    }

    /// Execute a k-NN search with a distance threshold.
    /// Only returns vectors within the specified maximum distance.
    pub fn search_within_radius(
        vectors: &ArrayRef,
        query: &[f32],
        radius: f32,
        metric: DistanceMetric,
    ) -> Result<KnnResult> {
        let dists = VectorOps::compute_distances(vectors, query, metric)?;
        let mut results: Vec<(usize, f32)> = (0..dists.len())
            .filter_map(|i| {
                if dists.is_valid(i) {
                    let d = dists.value(i);
                    if d <= radius {
                        Some((i, d))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let indices = results.iter().map(|(i, _)| *i).collect();
        let distances = results.iter().map(|(_, d)| *d).collect();

        Ok(KnnResult { indices, distances })
    }
}

// ---------------------------------------------------------------------------
// HNSW (Hierarchical Navigable Small World) Index
// ---------------------------------------------------------------------------

/// Configuration for HNSW index construction.
#[derive(Debug, Clone)]
pub struct HnswConfig {
    /// Maximum number of connections per node at layer 0.
    pub m: usize,
    /// Maximum number of connections per node at layers > 0.
    pub m_max: usize,
    /// Size of the dynamic candidate list during construction.
    pub ef_construction: usize,
    /// Size of the dynamic candidate list during search.
    pub ef_search: usize,
    /// Normalization factor for level generation (1/ln(M)).
    pub ml: f64,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            m: 16,
            m_max: 16,
            ef_construction: 200,
            ef_search: 50,
            ml: 1.0 / (16.0f64).ln(),
        }
    }
}

/// A node in the HNSW graph.
#[derive(Debug, Clone)]
struct HnswNode {
    vector: Vec<f32>,
    /// Connections at each layer: layer -> list of neighbor node IDs.
    connections: Vec<Vec<usize>>,
}

/// HNSW (Hierarchical Navigable Small World) index for approximate nearest neighbor search.
#[derive(Debug)]
pub struct HnswIndex {
    nodes: Vec<HnswNode>,
    config: HnswConfig,
    metric: DistanceMetric,
    entry_point: Option<usize>,
    max_layer: usize,
}

impl HnswIndex {
    /// Create a new empty HNSW index.
    pub fn new(metric: DistanceMetric, config: HnswConfig) -> Self {
        Self {
            nodes: Vec::new(),
            config,
            metric,
            entry_point: None,
            max_layer: 0,
        }
    }

    /// Create an HNSW index with default configuration.
    pub fn with_defaults(metric: DistanceMetric) -> Self {
        Self::new(metric, HnswConfig::default())
    }

    /// Number of vectors in the index.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Generate a random level for a new node.
    fn random_level(&self) -> usize {
        let r: f64 = rand_f64();
        (-r.ln() * self.config.ml).floor() as usize
    }

    /// Compute distance between a query and a stored node.
    fn distance_to_node(&self, query: &[f32], node_id: usize) -> Result<f32> {
        match self.metric {
            DistanceMetric::L2 => l2_distance(query, &self.nodes[node_id].vector),
            DistanceMetric::Cosine => cosine_distance(query, &self.nodes[node_id].vector),
            DistanceMetric::DotProduct | DistanceMetric::InnerProduct => {
                Ok(-dot_product(query, &self.nodes[node_id].vector)?)
            }
        }
    }

    /// Insert a vector into the index.
    pub fn insert(&mut self, vector: Vec<f32>) -> Result<usize> {
        let node_id = self.nodes.len();
        let level = self.random_level();

        // Create node with empty connections for each layer
        let node = HnswNode {
            vector,
            connections: vec![Vec::new(); level + 1],
        };
        self.nodes.push(node);

        if self.entry_point.is_none() {
            self.entry_point = Some(node_id);
            self.max_layer = level;
            return Ok(node_id);
        }

        let entry = self.entry_point.unwrap();
        let mut current = entry;

        // Traverse from top layer down to level+1, greedily finding closest node
        for layer in (level + 1..=self.max_layer).rev() {
            current = self.greedy_search_layer(&self.nodes[node_id].vector, current, layer)?;
        }

        // For layers level..0, find ef_construction nearest neighbors and connect
        for layer in (0..=level.min(self.max_layer)).rev() {
            let neighbors = self.search_layer(
                &self.nodes[node_id].vector,
                current,
                self.config.ef_construction,
                layer,
            )?;

            let m = if layer == 0 {
                self.config.m * 2
            } else {
                self.config.m
            };
            let selected: Vec<usize> = neighbors.iter().take(m).map(|&(id, _)| id).collect();

            self.nodes[node_id].connections[layer] = selected.clone();

            // Add bidirectional connections
            for &neighbor_id in &selected {
                if layer < self.nodes[neighbor_id].connections.len() {
                    self.nodes[neighbor_id].connections[layer].push(node_id);
                    // Trim if over capacity
                    if self.nodes[neighbor_id].connections[layer].len() > m {
                        let nv = self.nodes[neighbor_id].vector.clone();
                        let mut conn_dists: Vec<(usize, f32)> = self.nodes[neighbor_id].connections
                            [layer]
                            .iter()
                            .filter_map(|&c| self.distance_to_node(&nv, c).ok().map(|d| (c, d)))
                            .collect();
                        conn_dists.sort_by(|a, b| {
                            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                        });
                        self.nodes[neighbor_id].connections[layer] =
                            conn_dists.into_iter().take(m).map(|(id, _)| id).collect();
                    }
                }
            }

            if !neighbors.is_empty() {
                current = neighbors[0].0;
            }
        }

        if level > self.max_layer {
            self.max_layer = level;
            self.entry_point = Some(node_id);
        }

        Ok(node_id)
    }

    /// Greedy search on a single layer to find the closest node.
    fn greedy_search_layer(&self, query: &[f32], entry: usize, layer: usize) -> Result<usize> {
        let mut current = entry;
        let mut current_dist = self.distance_to_node(query, current)?;

        loop {
            let mut changed = false;
            if layer < self.nodes[current].connections.len() {
                for &neighbor in &self.nodes[current].connections[layer] {
                    let d = self.distance_to_node(query, neighbor)?;
                    if d < current_dist {
                        current = neighbor;
                        current_dist = d;
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }

        Ok(current)
    }

    /// Search a layer for the ef nearest neighbors.
    fn search_layer(
        &self,
        query: &[f32],
        entry: usize,
        ef: usize,
        layer: usize,
    ) -> Result<Vec<(usize, f32)>> {
        let entry_dist = self.distance_to_node(query, entry)?;
        let mut visited = HashSet::new();
        visited.insert(entry);

        // candidates: min-heap by distance
        let mut candidates: BinaryHeap<std::cmp::Reverse<OrdF32Pair>> = BinaryHeap::new();
        candidates.push(std::cmp::Reverse(OrdF32Pair(entry_dist, entry)));

        // results: max-heap by distance (to track worst)
        let mut results: BinaryHeap<OrdF32Pair> = BinaryHeap::new();
        results.push(OrdF32Pair(entry_dist, entry));

        while let Some(std::cmp::Reverse(OrdF32Pair(c_dist, c_id))) = candidates.pop() {
            let worst_dist = results.peek().map(|p| p.0).unwrap_or(f32::INFINITY);
            if c_dist > worst_dist && results.len() >= ef {
                break;
            }

            if layer < self.nodes[c_id].connections.len() {
                for &neighbor in &self.nodes[c_id].connections[layer] {
                    if visited.insert(neighbor) {
                        let d = self.distance_to_node(query, neighbor)?;
                        let worst = results.peek().map(|p| p.0).unwrap_or(f32::INFINITY);

                        if d < worst || results.len() < ef {
                            candidates.push(std::cmp::Reverse(OrdF32Pair(d, neighbor)));
                            results.push(OrdF32Pair(d, neighbor));
                            if results.len() > ef {
                                results.pop();
                            }
                        }
                    }
                }
            }
        }

        let mut result_vec: Vec<(usize, f32)> = results.into_iter().map(|p| (p.1, p.0)).collect();
        result_vec.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        Ok(result_vec)
    }

    /// Serialize the index to bytes for persistence.
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        // Header: node count, max_layer, entry_point, metric
        bytes.extend_from_slice(&(self.nodes.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&(self.max_layer as u32).to_le_bytes());
        bytes.extend_from_slice(&(self.entry_point.unwrap_or(usize::MAX) as u64).to_le_bytes());
        bytes.extend_from_slice(&(self.metric as u8).to_le_bytes());

        for node in &self.nodes {
            // Vector dimension + data
            bytes.extend_from_slice(&(node.vector.len() as u32).to_le_bytes());
            for &v in &node.vector {
                bytes.extend_from_slice(&v.to_le_bytes());
            }
            // Number of layers
            bytes.extend_from_slice(&(node.connections.len() as u32).to_le_bytes());
            for layer_conns in &node.connections {
                bytes.extend_from_slice(&(layer_conns.len() as u32).to_le_bytes());
                for &conn in layer_conns {
                    bytes.extend_from_slice(&(conn as u64).to_le_bytes());
                }
            }
        }

        Ok(bytes)
    }

    /// Deserialize an index from bytes.
    pub fn deserialize(bytes: &[u8], config: HnswConfig) -> Result<Self> {
        if bytes.len() < 21 {
            return Err(BlazeError::invalid_argument(
                "Invalid HNSW index data: too short",
            ));
        }
        let mut pos = 0;

        let node_count = u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap()) as usize;
        pos += 8;
        let max_layer = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let ep = u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap()) as usize;
        pos += 8;
        let metric_byte = bytes[pos];
        pos += 1;

        let metric = match metric_byte {
            0 => DistanceMetric::L2,
            1 => DistanceMetric::Cosine,
            2 => DistanceMetric::DotProduct,
            _ => DistanceMetric::L2,
        };

        let entry_point = if ep == usize::MAX { None } else { Some(ep) };
        let mut nodes = Vec::with_capacity(node_count);

        for _ in 0..node_count {
            if pos + 4 > bytes.len() {
                return Err(BlazeError::invalid_argument("Truncated HNSW index data"));
            }
            let dim = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            let mut vector = Vec::with_capacity(dim);
            for _ in 0..dim {
                vector.push(f32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()));
                pos += 4;
            }

            let num_layers = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            let mut connections = Vec::with_capacity(num_layers);
            for _ in 0..num_layers {
                let num_conns =
                    u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                let mut conns = Vec::with_capacity(num_conns);
                for _ in 0..num_conns {
                    conns
                        .push(u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap()) as usize);
                    pos += 8;
                }
                connections.push(conns);
            }

            nodes.push(HnswNode {
                vector,
                connections,
            });
        }

        Ok(Self {
            nodes,
            config,
            metric,
            entry_point,
            max_layer,
        })
    }
}

impl VectorIndex for HnswIndex {
    fn search_knn(&self, query: &[f32], k: usize) -> Result<Vec<(usize, f32)>> {
        if self.is_empty() {
            return Ok(Vec::new());
        }

        let entry = self.entry_point.unwrap();
        let mut current = entry;

        // Traverse from top layer to layer 1
        for layer in (1..=self.max_layer).rev() {
            current = self.greedy_search_layer(query, current, layer)?;
        }

        // Search layer 0 with ef_search candidates
        let mut results = self.search_layer(query, current, self.config.ef_search.max(k), 0)?;
        results.truncate(k);
        Ok(results)
    }
}

/// Helper for ordered f32 pairs in BinaryHeap.
#[derive(Debug, Clone, Copy)]
struct OrdF32Pair(f32, usize);

impl PartialEq for OrdF32Pair {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}
impl Eq for OrdF32Pair {}

impl PartialOrd for OrdF32Pair {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrdF32Pair {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Simple deterministic pseudo-random for level generation (no external deps).
fn rand_f64() -> f64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEED: AtomicU64 = AtomicU64::new(0x12345678_9abcdef0);
    let mut s = SEED.load(Ordering::Relaxed);
    s ^= s << 13;
    s ^= s >> 7;
    s ^= s << 17;
    SEED.store(s, Ordering::Relaxed);
    (s as f64) / (u64::MAX as f64)
}

// ---------------------------------------------------------------------------
// Compute distance using a given metric (standalone helper)
// ---------------------------------------------------------------------------

/// Compute distance between two vectors using the specified metric.
fn compute_distance_metric(a: &[f32], b: &[f32], metric: DistanceMetric) -> Result<f32> {
    match metric {
        DistanceMetric::L2 => l2_distance(a, b),
        DistanceMetric::Cosine => cosine_distance(a, b),
        DistanceMetric::DotProduct | DistanceMetric::InnerProduct => Ok(-dot_product(a, b)?),
    }
}

// ---------------------------------------------------------------------------
// IVF (Inverted File) Index
// ---------------------------------------------------------------------------

/// Inverted File index using k-means clustering for fast approximate search.
///
/// Vectors are partitioned into `num_clusters` clusters via k-means.
/// At query time only the closest `nprobe` clusters are searched, trading
/// recall for speed.
#[derive(Debug, Clone)]
pub struct IvfIndex {
    centroids: Vec<Vec<f32>>,
    clusters: Vec<Vec<(usize, Vec<f32>)>>,
    metric: DistanceMetric,
    num_clusters: usize,
    next_id: usize,
}

impl IvfIndex {
    /// Create a new IVF index with `num_clusters` partitions.
    pub fn new(num_clusters: usize, metric: DistanceMetric) -> Self {
        Self {
            centroids: Vec::new(),
            clusters: Vec::new(),
            metric,
            num_clusters,
            next_id: 0,
        }
    }

    /// Train the index by running k-means clustering on the provided vectors.
    pub fn train(&mut self, vectors: &[Vec<f32>]) {
        if vectors.is_empty() || self.num_clusters == 0 {
            return;
        }
        let k = self.num_clusters.min(vectors.len());
        // Initialize centroids with first k vectors
        let mut centroids: Vec<Vec<f32>> = vectors.iter().take(k).cloned().collect();
        let dim = centroids[0].len();

        // Run k-means for a fixed number of iterations
        for _ in 0..20 {
            let mut assignments: Vec<Vec<usize>> = vec![Vec::new(); k];
            for (vi, v) in vectors.iter().enumerate() {
                let nearest = Self::nearest_centroid(v, &centroids, self.metric);
                assignments[nearest].push(vi);
            }
            // Update centroids
            for (ci, members) in assignments.iter().enumerate() {
                if members.is_empty() {
                    continue;
                }
                let mut new_centroid = vec![0.0f32; dim];
                for &mi in members {
                    for (j, val) in vectors[mi].iter().enumerate() {
                        new_centroid[j] += val;
                    }
                }
                let count = members.len() as f32;
                for val in &mut new_centroid {
                    *val /= count;
                }
                centroids[ci] = new_centroid;
            }
        }

        self.centroids = centroids;
        self.clusters = vec![Vec::new(); k];
    }

    /// Insert a vector with the given id, assigning it to the nearest cluster.
    pub fn insert(&mut self, id: usize, vector: &[f32]) {
        if self.centroids.is_empty() {
            // If not trained yet, create a single cluster
            if self.clusters.is_empty() {
                self.centroids.push(vector.to_vec());
                self.clusters.push(Vec::new());
            }
        }
        let cluster = Self::nearest_centroid(vector, &self.centroids, self.metric);
        self.clusters[cluster].push((id, vector.to_vec()));
        self.next_id = self.next_id.max(id + 1);
    }

    /// Search for the k nearest neighbors, probing `nprobe` closest clusters.
    pub fn search_knn(&self, query: &[f32], k: usize, nprobe: usize) -> Vec<(usize, f32)> {
        if self.centroids.is_empty() {
            return Vec::new();
        }
        // Find closest centroids
        let mut centroid_dists: Vec<(usize, f32)> = self
            .centroids
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let d = compute_distance_metric(query, c, self.metric).unwrap_or(f32::MAX);
                (i, d)
            })
            .collect();
        centroid_dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let probes = nprobe.min(centroid_dists.len());
        let mut candidates: Vec<(usize, f32)> = Vec::new();
        for &(ci, _) in centroid_dists.iter().take(probes) {
            for (id, vec) in &self.clusters[ci] {
                let d = compute_distance_metric(query, vec, self.metric).unwrap_or(f32::MAX);
                candidates.push((*id, d));
            }
        }
        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(k);
        candidates
    }

    fn nearest_centroid(vector: &[f32], centroids: &[Vec<f32>], metric: DistanceMetric) -> usize {
        centroids
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let d = compute_distance_metric(vector, c, metric).unwrap_or(f32::MAX);
                (i, d)
            })
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(i, _)| i)
            .unwrap_or(0)
    }
}

impl VectorIndex for IvfIndex {
    fn search_knn(&self, query: &[f32], k: usize) -> Result<Vec<(usize, f32)>> {
        // Default nprobe = sqrt(num_clusters), at least 1
        let nprobe = (self.num_clusters as f64).sqrt().ceil() as usize;
        Ok(self.search_knn(query, k, nprobe.max(1)))
    }
}

// ---------------------------------------------------------------------------
// Product Quantization
// ---------------------------------------------------------------------------

/// Product Quantizer for vector compression.
///
/// Splits each vector into `num_subspaces` sub-vectors and quantizes each
/// independently into `2^bits_per_code` centroids, dramatically reducing memory.
#[derive(Debug, Clone)]
pub struct ProductQuantizer {
    dimensions: usize,
    num_subspaces: usize,
    #[allow(dead_code)] // TODO: use for encoding validation
    bits_per_code: usize,
    sub_dim: usize,
    num_codes: usize,
    /// Codebooks: one per subspace, each containing `num_codes` centroids of size `sub_dim`.
    codebooks: Vec<Vec<Vec<f32>>>,
}

impl ProductQuantizer {
    /// Create a new Product Quantizer.
    ///
    /// `dimensions` must be divisible by `num_subspaces`.
    /// `bits_per_code` determines how many centroids per subspace (2^bits).
    pub fn new(dimensions: usize, num_subspaces: usize, bits_per_code: usize) -> Self {
        let sub_dim = dimensions / num_subspaces;
        let num_codes = 1usize << bits_per_code;
        // Initialize codebooks with evenly-spaced dummy centroids
        let codebooks = (0..num_subspaces)
            .map(|_| {
                (0..num_codes)
                    .map(|c| {
                        let val = c as f32 / num_codes as f32;
                        vec![val; sub_dim]
                    })
                    .collect()
            })
            .collect();
        Self {
            dimensions,
            num_subspaces,
            bits_per_code,
            sub_dim,
            num_codes,
            codebooks,
        }
    }

    /// Train codebooks from a set of training vectors using simple k-means.
    pub fn train(&mut self, vectors: &[Vec<f32>]) {
        if vectors.is_empty() {
            return;
        }
        for s in 0..self.num_subspaces {
            let start = s * self.sub_dim;
            let end = start + self.sub_dim;
            // Extract sub-vectors for this subspace
            let sub_vecs: Vec<Vec<f32>> = vectors.iter().map(|v| v[start..end].to_vec()).collect();
            self.codebooks[s] = Self::kmeans(&sub_vecs, self.num_codes, 15);
        }
    }

    /// Encode a vector into a compact byte representation.
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        let mut codes = Vec::with_capacity(self.num_subspaces);
        for s in 0..self.num_subspaces {
            let start = s * self.sub_dim;
            let end = start + self.sub_dim;
            let sub = &vector[start..end];
            let nearest = self.nearest_code(s, sub);
            codes.push(nearest as u8);
        }
        codes
    }

    /// Decode a compact representation back to an approximate vector.
    pub fn decode(&self, code: &[u8]) -> Vec<f32> {
        let mut result = Vec::with_capacity(self.dimensions);
        for (s, &c) in code.iter().enumerate() {
            if s < self.num_subspaces {
                result.extend_from_slice(&self.codebooks[s][c as usize]);
            }
        }
        result
    }

    /// Compute asymmetric distance between an original query and a PQ code.
    ///
    /// This is more accurate than symmetric distance because the query is not
    /// quantized — only the database vector is approximated.
    pub fn asymmetric_distance(&self, query: &[f32], code: &[u8]) -> f32 {
        let mut dist = 0.0f32;
        for (s, &c) in code.iter().enumerate() {
            if s >= self.num_subspaces {
                break;
            }
            let start = s * self.sub_dim;
            let end = start + self.sub_dim;
            let sub_query = &query[start..end];
            let centroid = &self.codebooks[s][c as usize];
            for (a, b) in sub_query.iter().zip(centroid.iter()) {
                dist += (a - b).powi(2);
            }
        }
        dist.sqrt()
    }

    fn nearest_code(&self, subspace: usize, sub_vector: &[f32]) -> usize {
        self.codebooks[subspace]
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let d: f32 = sub_vector
                    .iter()
                    .zip(c.iter())
                    .map(|(a, b)| (a - b).powi(2))
                    .sum();
                (i, d)
            })
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(i, _)| i)
            .unwrap_or(0)
    }

    fn kmeans(vectors: &[Vec<f32>], k: usize, iterations: usize) -> Vec<Vec<f32>> {
        if vectors.is_empty() || k == 0 {
            return Vec::new();
        }
        let actual_k = k.min(vectors.len());
        let dim = vectors[0].len();
        let mut centroids: Vec<Vec<f32>> = vectors.iter().take(actual_k).cloned().collect();

        for _ in 0..iterations {
            let mut assignments: Vec<Vec<usize>> = vec![Vec::new(); actual_k];
            for (vi, v) in vectors.iter().enumerate() {
                let nearest = centroids
                    .iter()
                    .enumerate()
                    .map(|(ci, c)| {
                        let d: f32 = v.iter().zip(c.iter()).map(|(a, b)| (a - b).powi(2)).sum();
                        (ci, d)
                    })
                    .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
                    .map(|(ci, _)| ci)
                    .unwrap_or(0);
                assignments[nearest].push(vi);
            }
            for (ci, members) in assignments.iter().enumerate() {
                if members.is_empty() {
                    continue;
                }
                let mut new_c = vec![0.0f32; dim];
                for &mi in members {
                    for (j, val) in vectors[mi].iter().enumerate() {
                        new_c[j] += val;
                    }
                }
                let count = members.len() as f32;
                for val in &mut new_c {
                    *val /= count;
                }
                centroids[ci] = new_c;
            }
        }
        // Pad with zero vectors if we had fewer than k unique
        while centroids.len() < k {
            centroids.push(vec![0.0; dim]);
        }
        centroids
    }
}

// ---------------------------------------------------------------------------
// Hybrid Search
// ---------------------------------------------------------------------------

/// Filter for metadata-driven search narrowing.
#[derive(Debug, Clone)]
pub enum MetadataFilter {
    /// Exact match: field == value.
    Eq(String, String),
    /// Range match: min <= field <= max.
    Range(String, f64, f64),
    /// Logical AND of multiple filters.
    And(Vec<MetadataFilter>),
    /// Logical OR of multiple filters.
    Or(Vec<MetadataFilter>),
}

/// A single search result combining vector distance with metadata.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub id: usize,
    pub distance: f32,
    pub metadata: std::collections::HashMap<String, String>,
}

/// Engine that combines vector similarity search with metadata filtering.
pub struct HybridSearchEngine {
    vector_index: Box<dyn VectorIndex>,
    metadata: std::collections::HashMap<usize, std::collections::HashMap<String, String>>,
}

impl HybridSearchEngine {
    /// Create a new hybrid search engine wrapping a vector index.
    pub fn new(vector_index: Box<dyn VectorIndex>) -> Self {
        Self {
            vector_index,
            metadata: std::collections::HashMap::new(),
        }
    }

    /// Associate metadata with a vector id.
    pub fn set_metadata(&mut self, id: usize, metadata: std::collections::HashMap<String, String>) {
        self.metadata.insert(id, metadata);
    }

    /// Search combining vector similarity and optional metadata filter.
    ///
    /// First retrieves a larger candidate set from the vector index, then
    /// applies the metadata filter and returns the top-k results.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        filter: Option<MetadataFilter>,
    ) -> Result<Vec<SearchResult>> {
        // Fetch more candidates to allow for filtering
        let fetch_k = if filter.is_some() { k * 10 } else { k };
        let candidates = self.vector_index.search_knn(query, fetch_k.max(k))?;

        let mut results: Vec<SearchResult> = candidates
            .into_iter()
            .filter(|(id, _)| match &filter {
                None => true,
                Some(f) => {
                    let meta = self.metadata.get(id);
                    Self::matches_filter(meta, f)
                }
            })
            .map(|(id, distance)| {
                let metadata = self.metadata.get(&id).cloned().unwrap_or_default();
                SearchResult {
                    id,
                    distance,
                    metadata,
                }
            })
            .collect();

        results.truncate(k);
        Ok(results)
    }

    fn matches_filter(
        meta: Option<&std::collections::HashMap<String, String>>,
        filter: &MetadataFilter,
    ) -> bool {
        let meta = match meta {
            Some(m) => m,
            None => return false,
        };
        match filter {
            MetadataFilter::Eq(key, value) => meta.get(key) == Some(value),
            MetadataFilter::Range(key, min, max) => meta
                .get(key)
                .and_then(|v| v.parse::<f64>().ok())
                .is_some_and(|v| v >= *min && v <= *max),
            MetadataFilter::And(filters) => {
                filters.iter().all(|f| Self::matches_filter(Some(meta), f))
            }
            MetadataFilter::Or(filters) => {
                filters.iter().any(|f| Self::matches_filter(Some(meta), f))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Vector Analytics
// ---------------------------------------------------------------------------

/// Analytical operations over collections of vectors.
pub struct VectorAnalytics;

impl VectorAnalytics {
    /// Compute the centroid (element-wise mean) of a set of vectors.
    pub fn centroid(vectors: &[&[f32]]) -> Vec<f32> {
        if vectors.is_empty() {
            return Vec::new();
        }
        let dim = vectors[0].len();
        let mut result = vec![0.0f32; dim];
        for v in vectors {
            for (i, val) in v.iter().enumerate() {
                result[i] += val;
            }
        }
        let n = vectors.len() as f32;
        for val in &mut result {
            *val /= n;
        }
        result
    }

    /// Compute the total variance across all dimensions.
    pub fn variance(vectors: &[&[f32]]) -> f32 {
        if vectors.is_empty() {
            return 0.0;
        }
        let centroid = Self::centroid(vectors);
        let n = vectors.len() as f32;
        vectors
            .iter()
            .map(|v| {
                v.iter()
                    .zip(centroid.iter())
                    .map(|(a, c)| (a - c).powi(2))
                    .sum::<f32>()
            })
            .sum::<f32>()
            / n
    }

    /// Identify outlier vectors whose distance to the centroid exceeds `threshold`.
    ///
    /// Returns the indices of outlier vectors.
    pub fn outliers(vectors: &[&[f32]], threshold: f32) -> Vec<usize> {
        if vectors.is_empty() {
            return Vec::new();
        }
        let centroid = Self::centroid(vectors);
        vectors
            .iter()
            .enumerate()
            .filter_map(|(i, v)| {
                let dist: f32 = v
                    .iter()
                    .zip(centroid.iter())
                    .map(|(a, c)| (a - c).powi(2))
                    .sum::<f32>();
                if dist.sqrt() > threshold {
                    Some(i)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Compute the pairwise distance matrix for a set of vectors.
    pub fn pairwise_distances(vectors: &[&[f32]], metric: DistanceMetric) -> Vec<Vec<f32>> {
        let n = vectors.len();
        let mut matrix = vec![vec![0.0f32; n]; n];
        for i in 0..n {
            for j in (i + 1)..n {
                let d = compute_distance_metric(vectors[i], vectors[j], metric).unwrap_or(0.0);
                matrix[i][j] = d;
                matrix[j][i] = d;
            }
        }
        matrix
    }
}

// ---------------------------------------------------------------------------
// Auto-Index Selection
// ---------------------------------------------------------------------------

/// Recommends the best vector index type based on dataset characteristics.
#[derive(Debug)]
pub struct VectorIndexSelector;

/// Recommendation for which index type to use.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexRecommendation {
    pub index_type: RecommendedIndex,
    pub reason: String,
    pub estimated_build_time_ms: u64,
    pub estimated_memory_bytes: u64,
    pub expected_recall: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RecommendedIndex {
    BruteForce,
    Hnsw,
    Ivf,
    HnswWithPq,
}

impl VectorIndexSelector {
    /// Recommend an index type based on dataset size and dimensionality.
    pub fn recommend(num_vectors: usize, dimensions: usize) -> IndexRecommendation {
        if num_vectors < 1_000 {
            IndexRecommendation {
                index_type: RecommendedIndex::BruteForce,
                reason: "Small dataset (<1K vectors): brute force is fast enough".into(),
                estimated_build_time_ms: 0,
                estimated_memory_bytes: (num_vectors * dimensions * 4) as u64,
                expected_recall: 1.0,
            }
        } else if num_vectors < 100_000 {
            let memory = (num_vectors * dimensions * 4)
                + (num_vectors * 16 * std::mem::size_of::<usize>() * 5); // HNSW graph overhead
            IndexRecommendation {
                index_type: RecommendedIndex::Hnsw,
                reason: "Medium dataset (1K-100K): HNSW provides best latency/recall tradeoff"
                    .into(),
                estimated_build_time_ms: (num_vectors as u64) / 10,
                estimated_memory_bytes: memory as u64,
                expected_recall: 0.95,
            }
        } else if num_vectors < 10_000_000 {
            let memory = (num_vectors * dimensions * 4)
                + (num_vectors * 16 * std::mem::size_of::<usize>() * 5);
            IndexRecommendation {
                index_type: RecommendedIndex::Hnsw,
                reason: "Large dataset (100K-10M): HNSW with tuned ef for good recall".into(),
                estimated_build_time_ms: (num_vectors as u64) / 5,
                estimated_memory_bytes: memory as u64,
                expected_recall: 0.92,
            }
        } else {
            let pq_bytes = 32; // compressed vector size
            let memory = num_vectors * pq_bytes + num_vectors * 4; // PQ vectors + cluster assignments
            IndexRecommendation {
                index_type: RecommendedIndex::HnswWithPq,
                reason: "Very large dataset (>10M): IVF+PQ for memory efficiency".into(),
                estimated_build_time_ms: (num_vectors as u64) / 2,
                estimated_memory_bytes: memory as u64,
                expected_recall: 0.85,
            }
        }
    }

    /// Recommend HNSW parameters based on desired recall and dataset size.
    pub fn recommend_hnsw_params(num_vectors: usize, target_recall: f64) -> HnswConfig {
        let m = if target_recall > 0.95 {
            32
        } else if target_recall > 0.9 {
            16
        } else {
            8
        };
        let ef_construction = if target_recall > 0.95 {
            400
        } else if target_recall > 0.9 {
            200
        } else {
            100
        };
        let ef_search = if num_vectors > 1_000_000 { 200 } else { 100 };

        HnswConfig {
            m,
            ef_construction,
            ef_search,
            ..HnswConfig::default()
        }
    }
}

// ---------------------------------------------------------------------------
// Vector Index Serialization
// ---------------------------------------------------------------------------

/// Serializable vector index metadata for persistence.
#[derive(Debug, Clone)]
pub struct VectorIndexMeta {
    pub name: String,
    pub index_type: String,
    pub metric: DistanceMetric,
    pub dimensions: usize,
    pub num_vectors: usize,
    pub build_time_ms: u64,
    pub memory_bytes: u64,
}

/// Manages multiple vector indexes.
#[derive(Debug)]
pub struct VectorIndexManager {
    indexes: std::collections::HashMap<String, VectorIndexEntry>,
}

#[derive(Debug)]
struct VectorIndexEntry {
    index: Box<dyn VectorIndex>,
    meta: VectorIndexMeta,
}

impl std::fmt::Debug for dyn VectorIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VectorIndex")
    }
}

impl Default for VectorIndexManager {
    fn default() -> Self {
        Self::new()
    }
}

impl VectorIndexManager {
    pub fn new() -> Self {
        Self {
            indexes: std::collections::HashMap::new(),
        }
    }

    /// Create and register a new HNSW index.
    pub fn create_hnsw(
        &mut self,
        name: &str,
        metric: DistanceMetric,
        dimensions: usize,
        config: Option<HnswConfig>,
    ) -> &mut dyn VectorIndex {
        let cfg = config.unwrap_or_default();
        let index = HnswIndex::new(metric, cfg);
        let meta = VectorIndexMeta {
            name: name.to_string(),
            index_type: "hnsw".to_string(),
            metric,
            dimensions,
            num_vectors: 0,
            build_time_ms: 0,
            memory_bytes: 0,
        };
        self.indexes.insert(
            name.to_string(),
            VectorIndexEntry {
                index: Box::new(index),
                meta,
            },
        );
        self.indexes.get_mut(name).unwrap().index.as_mut()
    }

    /// Create and register a new brute force index.
    pub fn create_brute_force(
        &mut self,
        name: &str,
        metric: DistanceMetric,
        dimensions: usize,
    ) -> &mut dyn VectorIndex {
        let index = BruteForceIndex::new(metric);
        let meta = VectorIndexMeta {
            name: name.to_string(),
            index_type: "brute_force".to_string(),
            metric,
            dimensions,
            num_vectors: 0,
            build_time_ms: 0,
            memory_bytes: 0,
        };
        self.indexes.insert(
            name.to_string(),
            VectorIndexEntry {
                index: Box::new(index),
                meta,
            },
        );
        self.indexes.get_mut(name).unwrap().index.as_mut()
    }

    /// Get an index by name.
    pub fn get_index(&self, name: &str) -> Option<&dyn VectorIndex> {
        self.indexes.get(name).map(|e| e.index.as_ref())
    }

    /// Get index metadata.
    pub fn get_meta(&self, name: &str) -> Option<&VectorIndexMeta> {
        self.indexes.get(name).map(|e| &e.meta)
    }

    /// Drop an index.
    pub fn drop_index(&mut self, name: &str) -> bool {
        self.indexes.remove(name).is_some()
    }

    /// List all index names.
    pub fn list_indexes(&self) -> Vec<&str> {
        self.indexes.keys().map(|s| s.as_str()).collect()
    }

    /// Total number of indexes.
    pub fn index_count(&self) -> usize {
        self.indexes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Distance metric tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_l2_distance_basic() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        let d = l2_distance(&a, &b).unwrap();
        assert!((d - std::f32::consts::SQRT_2).abs() < 1e-6);
    }

    #[test]
    fn test_l2_distance_identical() {
        let a = vec![1.0, 2.0, 3.0];
        let d = l2_distance(&a, &a).unwrap();
        assert!((d - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_l2_distance_dimension_mismatch() {
        let a = vec![1.0, 2.0];
        let b = vec![1.0];
        assert!(l2_distance(&a, &b).is_err());
    }

    #[test]
    fn test_cosine_similarity_identical() {
        let a = vec![1.0, 2.0, 3.0];
        let sim = cosine_similarity(&a, &a).unwrap();
        assert!((sim - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_orthogonal() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        let sim = cosine_similarity(&a, &b).unwrap();
        assert!(sim.abs() < 1e-6);
    }

    #[test]
    fn test_cosine_distance_basic() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        let d = cosine_distance(&a, &b).unwrap();
        assert!((d - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_zero_vector() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![1.0, 2.0, 3.0];
        let sim = cosine_similarity(&a, &b).unwrap();
        assert!((sim - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_dot_product_basic() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        let d = dot_product(&a, &b).unwrap();
        assert!((d - 32.0).abs() < 1e-6);
    }

    #[test]
    fn test_dot_product_orthogonal() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        let d = dot_product(&a, &b).unwrap();
        assert!(d.abs() < 1e-6);
    }

    // -----------------------------------------------------------------------
    // BruteForceIndex tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_brute_force_knn_l2() {
        let mut index = BruteForceIndex::new(DistanceMetric::L2);
        index.add(vec![0.0, 0.0]);
        index.add(vec![1.0, 0.0]);
        index.add(vec![0.0, 1.0]);
        index.add(vec![10.0, 10.0]);

        let results = index.search_knn(&[0.0, 0.0], 2).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 0); // closest is itself
                                     // second closest is either (1,0) or (0,1) at distance 1.0
        assert!((results[1].1 - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_brute_force_knn_cosine() {
        let mut index = BruteForceIndex::new(DistanceMetric::Cosine);
        index.add(vec![1.0, 0.0]);
        index.add(vec![0.0, 1.0]);
        index.add(vec![1.0, 1.0]);

        // Query along x-axis; (1,0) should be closest
        let results = index.search_knn(&[1.0, 0.0], 1).unwrap();
        assert_eq!(results[0].0, 0);
        assert!(results[0].1.abs() < 1e-6);
    }

    #[test]
    fn test_brute_force_empty() {
        let index = BruteForceIndex::new(DistanceMetric::L2);
        assert!(index.is_empty());
        let results = index.search_knn(&[1.0, 2.0], 5).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_brute_force_k_larger_than_index() {
        let mut index = BruteForceIndex::new(DistanceMetric::L2);
        index.add(vec![1.0, 2.0]);
        let results = index.search_knn(&[1.0, 2.0], 10).unwrap();
        assert_eq!(results.len(), 1);
    }

    // -----------------------------------------------------------------------
    // VectorOps Arrow tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_compute_distances_l2() {
        let vecs = vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![1.0, 1.0]];
        let arr = VectorOps::build_vector_array(&vecs, 2).unwrap();
        let query = vec![0.0, 0.0];
        let dists = VectorOps::compute_distances(&arr, &query, DistanceMetric::L2).unwrap();
        assert_eq!(dists.len(), 3);
        assert!((dists.value(0) - 1.0).abs() < 1e-6);
        assert!((dists.value(1) - 1.0).abs() < 1e-6);
        assert!((dists.value(2) - std::f32::consts::SQRT_2).abs() < 1e-6);
    }

    #[test]
    fn test_compute_distances_cosine() {
        let vecs = vec![vec![1.0, 0.0], vec![0.0, 1.0]];
        let arr = VectorOps::build_vector_array(&vecs, 2).unwrap();
        let query = vec![1.0, 0.0];
        let dists = VectorOps::compute_distances(&arr, &query, DistanceMetric::Cosine).unwrap();
        assert!((dists.value(0) - 0.0).abs() < 1e-6); // identical direction
        assert!((dists.value(1) - 1.0).abs() < 1e-6); // orthogonal
    }

    #[test]
    fn test_compute_distances_dimension_mismatch() {
        let vecs = vec![vec![1.0, 0.0]];
        let arr = VectorOps::build_vector_array(&vecs, 2).unwrap();
        let query = vec![1.0, 0.0, 0.0];
        assert!(VectorOps::compute_distances(&arr, &query, DistanceMetric::L2).is_err());
    }

    #[test]
    fn test_top_k() {
        let distances = Float32Array::from(vec![5.0, 1.0, 3.0, 0.5, 2.0]);
        let top = VectorOps::top_k(&distances, 3).unwrap();
        assert_eq!(top.len(), 3);
        assert_eq!(top[0].0, 3); // 0.5
        assert_eq!(top[1].0, 1); // 1.0
        assert_eq!(top[2].0, 4); // 2.0
    }

    #[test]
    fn test_top_k_with_nan() {
        let distances = Float32Array::from(vec![Some(5.0), None, Some(1.0)]);
        let top = VectorOps::top_k(&distances, 5).unwrap();
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].0, 2); // 1.0
        assert_eq!(top[1].0, 0); // 5.0
    }

    #[test]
    fn test_build_vector_array_roundtrip() {
        let vecs = vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]];
        let arr = VectorOps::build_vector_array(&vecs, 3).unwrap();
        let fsl = arr.as_fixed_size_list();
        assert_eq!(fsl.len(), 2);
        assert_eq!(fsl.value_length(), 3);
    }

    // -----------------------------------------------------------------------
    // SIMD distance tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_l2_distance_simd_basic() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        let d = l2_distance_simd(&a, &b).unwrap();
        assert!((d - std::f32::consts::SQRT_2).abs() < 1e-5);
    }

    #[test]
    fn test_l2_distance_simd_large() {
        let a: Vec<f32> = (0..128).map(|i| i as f32).collect();
        let b: Vec<f32> = (0..128).map(|i| (i + 1) as f32).collect();
        let scalar = l2_distance(&a, &b).unwrap();
        let simd = l2_distance_simd(&a, &b).unwrap();
        assert!((scalar - simd).abs() < 1e-3);
    }

    #[test]
    fn test_dot_product_simd_basic() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        let d = dot_product_simd(&a, &b).unwrap();
        assert!((d - 32.0).abs() < 1e-5);
    }

    #[test]
    fn test_dot_product_simd_large() {
        let a: Vec<f32> = (0..256).map(|i| i as f32 * 0.1).collect();
        let b: Vec<f32> = (0..256).map(|i| i as f32 * 0.2).collect();
        let scalar = dot_product(&a, &b).unwrap();
        let simd = dot_product_simd(&a, &b).unwrap();
        assert!((scalar - simd).abs() / scalar.abs().max(1.0) < 1e-4);
    }

    #[test]
    fn test_batch_distances_simd() {
        let query = vec![0.0, 0.0];
        let vecs = vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![3.0, 4.0]];
        let dists = batch_distances_simd(&query, &vecs, DistanceMetric::L2).unwrap();
        assert_eq!(dists.len(), 3);
        assert!((dists[0] - 1.0).abs() < 1e-5);
        assert!((dists[1] - 1.0).abs() < 1e-5);
        assert!((dists[2] - 5.0).abs() < 1e-5);
    }

    // -----------------------------------------------------------------------
    // VectorKnnSearch tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_knn_search() {
        let vecs = vec![
            vec![0.0, 0.0],
            vec![1.0, 0.0],
            vec![0.0, 1.0],
            vec![10.0, 10.0],
        ];
        let arr = VectorOps::build_vector_array(&vecs, 2).unwrap();
        let result = VectorKnnSearch::search(&arr, &[0.0, 0.0], 2, DistanceMetric::L2).unwrap();
        assert_eq!(result.indices.len(), 2);
        assert_eq!(result.indices[0], 0);
        assert!((result.distances[0] - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_knn_search_within_radius() {
        let vecs = vec![
            vec![0.0, 0.0],
            vec![1.0, 0.0],
            vec![0.0, 1.0],
            vec![10.0, 10.0],
        ];
        let arr = VectorOps::build_vector_array(&vecs, 2).unwrap();
        let result =
            VectorKnnSearch::search_within_radius(&arr, &[0.0, 0.0], 1.5, DistanceMetric::L2)
                .unwrap();
        assert_eq!(result.indices.len(), 3); // [0,0], [1,0], [0,1] all within 1.5
        assert!(!result.indices.contains(&3)); // [10,10] is too far
    }

    // -----------------------------------------------------------------------
    // HNSW index tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_hnsw_empty() {
        let index = HnswIndex::with_defaults(DistanceMetric::L2);
        assert!(index.is_empty());
        let results = index.search_knn(&[1.0, 2.0], 5).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_hnsw_single_element() {
        let mut index = HnswIndex::with_defaults(DistanceMetric::L2);
        index.insert(vec![1.0, 2.0]).unwrap();
        assert_eq!(index.len(), 1);
        let results = index.search_knn(&[1.0, 2.0], 1).unwrap();
        assert_eq!(results.len(), 1);
        assert!((results[0].1 - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_hnsw_knn_basic() {
        let mut index = HnswIndex::with_defaults(DistanceMetric::L2);
        index.insert(vec![0.0, 0.0]).unwrap();
        index.insert(vec![1.0, 0.0]).unwrap();
        index.insert(vec![0.0, 1.0]).unwrap();
        index.insert(vec![10.0, 10.0]).unwrap();
        index.insert(vec![5.0, 5.0]).unwrap();

        let results = index.search_knn(&[0.0, 0.0], 2).unwrap();
        assert_eq!(results.len(), 2);
        // Closest should be (0,0) itself
        assert_eq!(results[0].0, 0);
    }

    #[test]
    fn test_hnsw_larger_dataset() {
        let config = HnswConfig {
            m: 8,
            m_max: 8,
            ef_construction: 50,
            ef_search: 30,
            ..HnswConfig::default()
        };
        let mut index = HnswIndex::new(DistanceMetric::L2, config);

        for i in 0..100 {
            let x = (i as f32) * 0.1;
            let y = (i as f32) * 0.2;
            index.insert(vec![x, y]).unwrap();
        }

        assert_eq!(index.len(), 100);
        let results = index.search_knn(&[0.0, 0.0], 5).unwrap();
        assert_eq!(results.len(), 5);
        // First result should be the origin (0,0) = index 0
        assert_eq!(results[0].0, 0);
    }

    #[test]
    fn test_hnsw_serialize_deserialize() {
        let mut index = HnswIndex::with_defaults(DistanceMetric::L2);
        index.insert(vec![1.0, 2.0]).unwrap();
        index.insert(vec![3.0, 4.0]).unwrap();
        index.insert(vec![5.0, 6.0]).unwrap();

        let bytes = index.serialize().unwrap();
        let restored = HnswIndex::deserialize(&bytes, HnswConfig::default()).unwrap();

        assert_eq!(restored.len(), 3);
        let results = restored.search_knn(&[1.0, 2.0], 1).unwrap();
        assert_eq!(results.len(), 1);
        assert!((results[0].1 - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_hnsw_cosine_metric() {
        let mut index = HnswIndex::with_defaults(DistanceMetric::Cosine);
        index.insert(vec![1.0, 0.0]).unwrap();
        index.insert(vec![0.0, 1.0]).unwrap();
        index.insert(vec![1.0, 1.0]).unwrap();

        let results = index.search_knn(&[1.0, 0.0], 1).unwrap();
        assert_eq!(results[0].0, 0); // Same direction
    }

    // -----------------------------------------------------------------------
    // IVF Index tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_ivf_train_and_search() {
        let vectors = vec![
            vec![1.0, 0.0],
            vec![1.1, 0.1],
            vec![0.0, 1.0],
            vec![0.1, 1.1],
            vec![5.0, 5.0],
            vec![5.1, 5.1],
        ];
        let mut index = IvfIndex::new(2, DistanceMetric::L2);
        index.train(&vectors);
        for (i, v) in vectors.iter().enumerate() {
            index.insert(i, v);
        }
        let results = index.search_knn(&[1.0, 0.0], 2, 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 0);
    }

    #[test]
    fn test_ivf_nprobe_affects_results() {
        let mut vectors = Vec::new();
        for i in 0..20 {
            vectors.push(vec![i as f32, 0.0]);
        }
        let mut index = IvfIndex::new(4, DistanceMetric::L2);
        index.train(&vectors);
        for (i, v) in vectors.iter().enumerate() {
            index.insert(i, v);
        }
        // Searching with more probes should return at least as good results
        let r1 = index.search_knn(&[10.0, 0.0], 3, 1);
        let r4 = index.search_knn(&[10.0, 0.0], 3, 4);
        assert!(!r1.is_empty());
        assert!(!r4.is_empty());
        // nprobe=4 (all clusters) should find the true nearest
        assert_eq!(r4[0].0, 10);
    }

    #[test]
    fn test_ivf_vector_index_trait() {
        let vectors = vec![vec![0.0, 0.0], vec![1.0, 1.0], vec![2.0, 2.0]];
        let mut index = IvfIndex::new(2, DistanceMetric::L2);
        index.train(&vectors);
        for (i, v) in vectors.iter().enumerate() {
            index.insert(i, v);
        }
        // Use the VectorIndex trait method
        let results = VectorIndex::search_knn(&index, &[0.0, 0.0], 1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 0);
    }

    // -----------------------------------------------------------------------
    // Product Quantization tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_pq_encode_decode_roundtrip() {
        let pq = ProductQuantizer::new(4, 2, 4);
        let vector = vec![0.5, 0.5, 0.5, 0.5];
        let code = pq.encode(&vector);
        assert_eq!(code.len(), 2);
        let decoded = pq.decode(&code);
        assert_eq!(decoded.len(), 4);
    }

    #[test]
    fn test_pq_trained_accuracy() {
        let mut pq = ProductQuantizer::new(4, 2, 4);
        let training: Vec<Vec<f32>> = (0..64)
            .map(|i| {
                let v = i as f32 / 64.0;
                vec![v, v, v, v]
            })
            .collect();
        pq.train(&training);
        let original = vec![0.25, 0.25, 0.25, 0.25];
        let code = pq.encode(&original);
        let decoded = pq.decode(&code);
        // After training, reconstruction error should be small
        let error: f32 = original
            .iter()
            .zip(decoded.iter())
            .map(|(a, b)| (a - b).powi(2))
            .sum::<f32>()
            .sqrt();
        assert!(error < 0.2, "Reconstruction error too large: {}", error);
    }

    #[test]
    fn test_pq_asymmetric_distance() {
        let pq = ProductQuantizer::new(4, 2, 4);
        let query = vec![0.5, 0.5, 0.5, 0.5];
        let code = pq.encode(&query);
        let dist = pq.asymmetric_distance(&query, &code);
        // Distance from a vector to its own code should be small
        let decoded = pq.decode(&code);
        let expected: f32 = query
            .iter()
            .zip(decoded.iter())
            .map(|(a, b)| (a - b).powi(2))
            .sum::<f32>()
            .sqrt();
        assert!((dist - expected).abs() < 1e-6);
    }

    // -----------------------------------------------------------------------
    // Hybrid Search tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_hybrid_search_no_filter() {
        let mut bf = BruteForceIndex::new(DistanceMetric::L2);
        bf.add(vec![0.0, 0.0]);
        bf.add(vec![1.0, 1.0]);
        bf.add(vec![2.0, 2.0]);
        let engine = HybridSearchEngine::new(Box::new(bf));
        let results = engine.search(&[0.0, 0.0], 2, None).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, 0);
    }

    #[test]
    fn test_hybrid_search_with_eq_filter() {
        let mut bf = BruteForceIndex::new(DistanceMetric::L2);
        bf.add(vec![0.0, 0.0]);
        bf.add(vec![0.1, 0.1]);
        bf.add(vec![10.0, 10.0]);

        let mut engine = HybridSearchEngine::new(Box::new(bf));
        let mut meta0 = std::collections::HashMap::new();
        meta0.insert("category".to_string(), "A".to_string());
        engine.set_metadata(0, meta0);
        let mut meta1 = std::collections::HashMap::new();
        meta1.insert("category".to_string(), "B".to_string());
        engine.set_metadata(1, meta1);
        let mut meta2 = std::collections::HashMap::new();
        meta2.insert("category".to_string(), "A".to_string());
        engine.set_metadata(2, meta2);

        let filter = MetadataFilter::Eq("category".to_string(), "A".to_string());
        let results = engine.search(&[0.0, 0.0], 10, Some(filter)).unwrap();
        assert_eq!(results.len(), 2);
        assert!(results
            .iter()
            .all(|r| r.metadata.get("category") == Some(&"A".to_string())));
    }

    #[test]
    fn test_hybrid_search_with_and_filter() {
        let mut bf = BruteForceIndex::new(DistanceMetric::L2);
        bf.add(vec![0.0, 0.0]);
        bf.add(vec![1.0, 1.0]);

        let mut engine = HybridSearchEngine::new(Box::new(bf));
        let mut meta = std::collections::HashMap::new();
        meta.insert("type".to_string(), "doc".to_string());
        meta.insert("score".to_string(), "5.0".to_string());
        engine.set_metadata(0, meta);

        let mut meta1 = std::collections::HashMap::new();
        meta1.insert("type".to_string(), "doc".to_string());
        meta1.insert("score".to_string(), "15.0".to_string());
        engine.set_metadata(1, meta1);

        let filter = MetadataFilter::And(vec![
            MetadataFilter::Eq("type".to_string(), "doc".to_string()),
            MetadataFilter::Range("score".to_string(), 0.0, 10.0),
        ]);
        let results = engine.search(&[0.0, 0.0], 10, Some(filter)).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 0);
    }

    // -----------------------------------------------------------------------
    // Vector Analytics tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_analytics_centroid() {
        let v1: Vec<f32> = vec![0.0, 0.0];
        let v2: Vec<f32> = vec![2.0, 4.0];
        let vectors: Vec<&[f32]> = vec![&v1, &v2];
        let c = VectorAnalytics::centroid(&vectors);
        assert!((c[0] - 1.0).abs() < 1e-6);
        assert!((c[1] - 2.0).abs() < 1e-6);
    }

    #[test]
    fn test_analytics_variance() {
        let v1: Vec<f32> = vec![0.0, 0.0];
        let v2: Vec<f32> = vec![2.0, 0.0];
        let vectors: Vec<&[f32]> = vec![&v1, &v2];
        let var = VectorAnalytics::variance(&vectors);
        // centroid = [1,0], distances^2 = 1+1 = 2, var = 2/2 = 1
        assert!((var - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_analytics_outliers() {
        let v1: Vec<f32> = vec![0.0, 0.0];
        let v2: Vec<f32> = vec![1.0, 0.0];
        let v3: Vec<f32> = vec![0.5, 0.0];
        let v4: Vec<f32> = vec![100.0, 0.0];
        let vectors: Vec<&[f32]> = vec![&v1, &v2, &v3, &v4];
        // centroid ~[25.375, 0], v4 is ~74.6 away, others ~25 away
        let outlier_indices = VectorAnalytics::outliers(&vectors, 50.0);
        assert!(outlier_indices.contains(&3), "v4 should be an outlier");
        assert!(!outlier_indices.contains(&0), "v1 should not be an outlier");
    }

    #[test]
    fn test_analytics_pairwise_distances() {
        let v1: Vec<f32> = vec![0.0, 0.0];
        let v2: Vec<f32> = vec![3.0, 4.0];
        let vectors: Vec<&[f32]> = vec![&v1, &v2];
        let matrix = VectorAnalytics::pairwise_distances(&vectors, DistanceMetric::L2);
        assert_eq!(matrix.len(), 2);
        assert!((matrix[0][0] - 0.0).abs() < 1e-6);
        assert!((matrix[0][1] - 5.0).abs() < 1e-6);
        assert!((matrix[1][0] - 5.0).abs() < 1e-6);
    }

    // -----------------------------------------------------------------------
    // Auto-Index Selection tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_index_selector_small() {
        let rec = VectorIndexSelector::recommend(500, 128);
        assert_eq!(rec.index_type, RecommendedIndex::BruteForce);
        assert_eq!(rec.expected_recall, 1.0);
    }

    #[test]
    fn test_index_selector_medium() {
        let rec = VectorIndexSelector::recommend(50_000, 256);
        assert_eq!(rec.index_type, RecommendedIndex::Hnsw);
        assert!(rec.expected_recall >= 0.9);
    }

    #[test]
    fn test_index_selector_large() {
        let rec = VectorIndexSelector::recommend(50_000_000, 384);
        assert_eq!(rec.index_type, RecommendedIndex::HnswWithPq);
    }

    #[test]
    fn test_hnsw_param_recommendation() {
        let params = VectorIndexSelector::recommend_hnsw_params(1_000_000, 0.97);
        assert_eq!(params.m, 32);
        assert_eq!(params.ef_construction, 400);
    }

    // -----------------------------------------------------------------------
    // VectorIndexManager tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_index_manager_create_hnsw() {
        let mut manager = VectorIndexManager::new();
        manager.create_hnsw("test_idx", DistanceMetric::L2, 3, None);

        assert_eq!(manager.index_count(), 1);
        let meta = manager.get_meta("test_idx").unwrap();
        assert_eq!(meta.index_type, "hnsw");
        assert_eq!(meta.dimensions, 3);
    }

    #[test]
    fn test_index_manager_create_brute_force() {
        let mut manager = VectorIndexManager::new();
        manager.create_brute_force("bf_idx", DistanceMetric::Cosine, 4);

        assert!(manager.get_index("bf_idx").is_some());
        assert!(manager.get_index("missing").is_none());
    }

    #[test]
    fn test_index_manager_drop() {
        let mut manager = VectorIndexManager::new();
        manager.create_brute_force("idx1", DistanceMetric::L2, 2);
        manager.create_brute_force("idx2", DistanceMetric::L2, 2);
        assert_eq!(manager.index_count(), 2);

        assert!(manager.drop_index("idx1"));
        assert_eq!(manager.index_count(), 1);
        assert!(!manager.drop_index("idx1")); // already dropped
    }
}
