use std::sync::Mutex;

use arrow_buffer::ScalarBuffer;
use rand::seq::SliceRandom;

use crate::log;

pub const MAX_RANDOM_INDICES: usize = 10 * 1024 * 1024;

pub struct RandomIndices {
    indices: ScalarBuffer<u64>,
    current_index: Mutex<usize>,
}

impl RandomIndices {
    fn generate_indices(num_indices: usize) -> Vec<u64> {
        log(format!("Generating {} random indices", num_indices));
        let mut indices = (0..(num_indices) as u64).collect::<Vec<_>>();
        let mut rng = rand::thread_rng();
        if indices.len() > MAX_RANDOM_INDICES {
            // If there are more than MAX_INDICES indices, then randomly select MAX_INDICES
            // from the larger set (we still sample from the entire set) and run on that.  This
            // avoids massive shuffles for large numbers of indices.
            //
            // Note: MAX_INDICES must be large enough to avoid repeating indices in the duration
            // and so we might need to make it larger if playing with durations beyond 10 seconds.
            //
            // If we exceed the indices we will panic so we can catch this and increase MAX_INDICES
            let (shuffled, _) = indices.partial_shuffle(&mut rng, MAX_RANDOM_INDICES);
            shuffled.to_vec()
        } else {
            indices.shuffle(&mut rng);
            indices
        }
    }

    pub async fn new(rows_per_chunk: usize, num_chunks: usize) -> Self {
        // Sorting a billion indices is slow so we save the computation if we can
        let num_indices = rows_per_chunk * num_chunks;
        let indices = Self::generate_indices(num_indices);
        Self {
            indices: indices.into(),
            current_index: Mutex::new(0),
        }
    }

    pub fn next(&self, take_size: usize) -> ScalarBuffer<u64> {
        let mut current_index = self.current_index.lock().unwrap();
        let start = *current_index;

        if take_size + start > self.indices.len() {
            panic!("Not enough input data for duration.  Would repeat indices");
        } else {
            *current_index += take_size;
        };

        self.indices.slice(start, take_size)
    }

    pub fn all_indices(&self) -> ScalarBuffer<u64> {
        self.indices.clone()
    }
}
