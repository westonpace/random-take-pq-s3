//! Implementation of the "take" operation for Parquet

use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt};
use parquet::arrow::{
    arrow_reader::{
        ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection,
        RowSelector,
    },
    ProjectionMask,
};
use std::{fs::File, iter, sync::atomic::Ordering};

use crate::{parq::io::FileLike, TAKE_COUNTER};

/// An iterator of that starts with a list of file offsets and generates RowSelectors
struct IndicesToRowSelection<'a, I: Iterator<Item = &'a u32>> {
    iter: I,
    start: u32,
    end: u32,
    last: u32,
    next: Option<RowSelector>,
}

impl<'a, I: Iterator<Item = &'a u32>> Iterator for IndicesToRowSelection<'a, I> {
    type Item = RowSelector;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(next) = self.next.take() {
            return Some(next);
        }
        loop {
            let next_idx = self.iter.next();
            if let Some(next_idx) = next_idx {
                if *next_idx < self.start {
                    continue;
                }
                if *next_idx >= self.end {
                    return None;
                }
                let to_skip = *next_idx - self.last;
                self.last = *next_idx + 1;
                if to_skip > 0 {
                    self.next = Some(RowSelector::select(1));
                    return Some(RowSelector::skip(to_skip as usize));
                } else {
                    return Some(RowSelector::select(1));
                }
            } else {
                return None;
            }
        }
    }
}

/// Create a task that performs a take operation on single row group's worth of data
pub fn take_task<T: FileLike>(
    file: T,
    metadata: ArrowReaderMetadata,
    row_group_number: u32,
    row_indices: Vec<u32>,
    column_index: u32,
    use_selection: bool,
) {
    let start = if row_group_number == 0 {
        0
    } else {
        (0..row_group_number)
            .map(|rg_num| metadata.metadata().row_group(rg_num as usize).num_rows())
            .sum()
    };
    let end = start
        + metadata
            .metadata()
            .row_group(row_group_number as usize)
            .num_rows();

    let selection = IndicesToRowSelection {
        iter: row_indices.iter(),
        start: start as u32,
        end: end as u32,
        last: start as u32,
        next: None,
    };
    let selection = RowSelection::from_iter(selection);

    assert!(selection.selects_any());

    let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, metadata);
    let parquet_schema = builder.parquet_schema();
    let projection = ProjectionMask::roots(parquet_schema, iter::once(column_index as usize));
    let batch = if use_selection {
        let reader = builder
            .with_limit(row_indices.len())
            .with_row_groups(vec![row_group_number as usize])
            .with_row_selection(selection)
            .with_projection(projection)
            .build()
            .unwrap();
        reader
            .collect::<std::result::Result<Vec<_>, arrow_schema::ArrowError>>()
            .unwrap()
    } else {
        let reader = builder.with_projection(projection).build().unwrap();
        reader
            .collect::<std::result::Result<Vec<_>, arrow_schema::ArrowError>>()
            .unwrap()
    };
    let rows_taken = batch.iter().map(|batch| batch.num_rows()).sum::<usize>();
    assert_eq!(rows_taken, row_indices.len());
    TAKE_COUNTER.fetch_add(rows_taken, Ordering::Release);
}

pub trait TryClone {
    fn try_clone(&self) -> std::io::Result<Self>
    where
        Self: Sized;
}

impl TryClone for File {
    fn try_clone(&self) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        self.try_clone()
    }
}

fn take_with_metadata<T: FileLike>(
    file: T,
    row_indices: Vec<u32>, // Note: this is sorted
    column_index: u32,
    use_selection: bool,
    metadata: &ArrowReaderMetadata,
    task_pool: &mut FuturesUnordered<BoxFuture<'static, ()>>,
) {
    let mut row_group_number = 0;
    let mut indices_for_group = Vec::with_capacity(row_indices.len());
    let mut cur_row_group = metadata.metadata().row_group(row_group_number as usize);
    let mut offset = 0;
    for index in row_indices {
        let mut idx_in_grp = index - offset;
        while idx_in_grp >= cur_row_group.num_rows() as u32 {
            if !indices_for_group.is_empty() {
                let take_indices = indices_for_group.clone();
                let file = file.try_clone().unwrap();
                let metadata = metadata.clone();
                task_pool.push(
                    tokio::task::spawn_blocking(move || {
                        take_task(
                            file,
                            metadata,
                            row_group_number,
                            take_indices,
                            column_index,
                            use_selection,
                        )
                    })
                    .map(|res| res.unwrap())
                    .boxed(),
                );
                indices_for_group.clear();
            }
            offset += cur_row_group.num_rows() as u32;
            idx_in_grp -= cur_row_group.num_rows() as u32;
            row_group_number += 1;
            cur_row_group = metadata.metadata().row_group(row_group_number as usize);
        }
        indices_for_group.push(index);
    }
    if !indices_for_group.is_empty() {
        let take_indices = indices_for_group.clone();
        let file = file.try_clone().unwrap();
        let metadata = metadata.clone();
        task_pool.push(
            tokio::task::spawn_blocking(move || {
                take_task(
                    file,
                    metadata,
                    row_group_number,
                    take_indices,
                    column_index,
                    use_selection,
                )
            })
            .map(|res| res.unwrap())
            .boxed(),
        );
    }
}

pub fn take<T: FileLike>(
    file: T,
    row_indices: Vec<u32>,
    column_index: u32,
    use_selection: bool,
    metadata: Option<ArrowReaderMetadata>,
    task_pool: &mut FuturesUnordered<BoxFuture<'static, ()>>,
) {
    if let Some(metadata) = metadata {
        take_with_metadata(
            file,
            row_indices,
            column_index,
            use_selection,
            &metadata,
            task_pool,
        );
    } else {
        unimplemented!()
    }
}

pub fn scan_task<T: FileLike>(
    file: T,
    column_indices: &[u32],
    row_group_number: u32,
    metadata: ArrowReaderMetadata,
) -> u64 {
    let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, metadata);
    let parquet_schema = builder.parquet_schema();
    let projection = ProjectionMask::roots(
        parquet_schema,
        column_indices.iter().map(|col_idx| *col_idx as usize),
    );
    let reader = builder
        .with_projection(projection)
        .with_row_groups(vec![row_group_number as usize])
        .build()
        .unwrap();
    let mut num_rows = 0;
    for batch in reader {
        num_rows += batch.unwrap().num_rows() as u64;
    }
    num_rows
}

pub fn scan<T: FileLike>(
    file: T,
    column_indices: &[u32],
    metadata: Option<ArrowReaderMetadata>,
) -> u64 {
    std::thread::scope(|scope| {
        let metadata = metadata.unwrap_or_else(|| {
            let options = ArrowReaderOptions::new();
            ArrowReaderMetadata::load(&file, options).unwrap()
        });
        let task_handles = (0..metadata.metadata().num_row_groups())
            .map(|row_group_number| {
                let file = file.try_clone().unwrap();
                let metadata = metadata.clone();
                scope.spawn(move || {
                    scan_task(file, column_indices, row_group_number as u32, metadata)
                })
            })
            .collect::<Vec<_>>();

        task_handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .sum()
    })
}
