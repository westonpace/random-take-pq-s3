use core::str;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Instant,
};

use arrow_buffer::ScalarBuffer;
use clap::Parser;
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use random_take_bench::{
    log,
    osutil::drop_caches,
    parq::{
        io::{FileLike, ObjectStoreFile, ReadAtFile, WorkDir},
        parq_file_path, parquet_global_setup,
    },
    take::take,
    util::RandomIndices,
    DataTypeChoice, LOG_READS, SHOULD_LOG, TAKE_COUNTER,
};
use tracing::Level;
use tracing_chrome::ChromeLayerBuilder;
use tracing_core::LevelFilter;
use tracing_subscriber::prelude::*;

#[derive(Parser, Clone, Debug)]
#[command(name="random-take", about="A benchmark for tabular file formats", version, long_about = None)]
struct Args {
    /// How many rows to put in a row group (parquet only)
    #[arg(short, long, default_value_t = 100000)]
    row_group_size: usize,

    /// Page size (in bytes, parquet only)
    #[arg(short, long, default_value_t = 1024)]
    page_size_kb: usize,

    /// How many rows to take in each take operation
    #[arg(short, long, default_value_t = 1024)]
    take_size: usize,

    /// Which data_type to test with
    #[arg(long, value_enum, default_value_t = DataTypeChoice::Scalar)]
    data_type: DataTypeChoice,

    /// Whether or not metadata should be cached between takes
    #[arg(short, long, default_value_t = false)]
    cache_metadata: bool,

    /// (parquet only) Whether or not to use compression
    #[arg(short, long, default_value_t = false)]
    compression: bool,

    /// (parquet only) Whether or not to use dictionary encoding
    #[arg(short, long, default_value_t = false)]
    dictionary: bool,

    /// If true, use the async reader
    #[arg(short, long, default_value_t = false)]
    r#async: bool,

    /// Number of seconds to run the benchmark
    #[arg(long, default_value_t = 10.0)]
    duration_seconds: f64,

    /// If true, drop the OS cache before each iteration
    #[arg(short, long, default_value_t = false)]
    drop_caches: bool,

    /// If true, log each read operation
    #[arg(long, default_value_t = false)]
    log_reads: bool,

    /// If true, enable rust logging via env_logger, not compatible with tracing
    #[arg(long, default_value_t = false)]
    rust_logging: bool,

    /// Number of files to read from
    #[arg(short, long, default_value_t = 1024)]
    num_files: usize,

    /// Number of rows per file
    #[arg(short, long, default_value_t = 1024 * 1024)]
    rows_per_file: usize,

    /// If quiet then only print the result
    #[arg(short, long, default_value_t = false)]
    quiet: bool,

    /// The number of concurrent takes to run (note that each take may have its own per-row-group parallelism)
    #[arg(long, default_value_t = 0)]
    concurrency: usize,

    /// URI of the working directory.  Must be file:// or s3://
    #[arg(short, long)]
    workdir: Option<String>,

    /// If true, enables tracing
    #[arg(long, default_value_t = false)]
    tracing: bool,
}

fn inner_parquet_setup_sync<T: FileLike>(args: &Args, file: T) -> Option<ArrowReaderMetadata> {
    let options = ArrowReaderOptions::new().with_page_index(true);
    let metadata = ArrowReaderMetadata::load(&file, options).unwrap();
    let num_rows = metadata
        .metadata()
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows())
        .sum::<i64>();

    let offset_index = metadata.metadata().offset_index().unwrap();

    let col_0_num_pages = offset_index
        .iter()
        .map(|rg| rg[0].page_locations().len())
        .sum::<usize>();

    log(format!(
        "Parquet file with type {} has {} rows and {} pages across {} row groups",
        args.data_type,
        num_rows,
        col_0_num_pages,
        metadata.metadata().num_row_groups()
    ));

    let metadata = if args.cache_metadata {
        Some(metadata)
    } else {
        None
    };

    metadata
}

fn parquet_setup_sync(args: &Args, work_dir: &WorkDir) -> Vec<Option<ArrowReaderMetadata>> {
    let mut metadata_lookup = Vec::with_capacity(args.num_files);
    for chunk_index in 0..args.num_files {
        let path = parq_file_path(
            work_dir,
            args.row_group_size,
            args.page_size_kb,
            chunk_index,
            args.data_type,
            args.compression,
            args.dictionary,
        );
        if work_dir.is_s3() {
            let file = work_dir.s3_file(path);
            metadata_lookup.push(inner_parquet_setup_sync(args, file));
        } else {
            let file = work_dir.local_file(path);
            metadata_lookup.push(inner_parquet_setup_sync(args, file));
        }
    }
    metadata_lookup
}

fn parquet_random_take<T: FileLike>(
    files: &[T],
    rows_per_file: usize,
    indices: ScalarBuffer<u64>,
    task_pool: &mut FuturesUnordered<BoxFuture<'static, ()>>,
    metadata_lookup: &[Option<ArrowReaderMetadata>],
    col: u32,
) {
    let mut indices = indices.to_vec();
    indices.sort_unstable();

    let mut indices_for_file = Vec::with_capacity(indices.len());
    let mut current_chunk = 0;
    for index in indices {
        let chunk_index = index / rows_per_file as u64;
        let chunk_offset = (index % rows_per_file as u64) as u32;
        if chunk_index == current_chunk {
            indices_for_file.push(chunk_offset);
        } else {
            if !indices_for_file.is_empty() {
                let file = files[current_chunk as usize].try_clone().unwrap();
                let task_indices = indices_for_file.clone();
                let metadata = metadata_lookup[current_chunk as usize].clone();
                take(file, task_indices, col, true, metadata, task_pool);
                indices_for_file.clear();
            }
            current_chunk = chunk_index;
            indices_for_file.push(chunk_offset);
        }
    }
    if !indices_for_file.is_empty() {
        let file = files[current_chunk as usize].try_clone().unwrap();
        let indices_for_file = indices_for_file.clone();
        let metadata = metadata_lookup[current_chunk as usize].clone();
        take(
            file,
            indices_for_file.into(),
            col,
            true,
            metadata,
            task_pool,
        );
    }
}

fn bench_parquet_one<T: FileLike>(
    args: &Args,
    indices: &RandomIndices,
    files: &[T],
    metadata_lookup: &[Option<ArrowReaderMetadata>],
) -> BoxFuture<'static, ()> {
    let args = args.clone();
    let indices = indices.next(args.take_size);
    let mut task_pool = FuturesUnordered::new();

    parquet_random_take(
        files,
        args.rows_per_file,
        indices,
        &mut task_pool,
        metadata_lookup,
        0,
    );

    async move { while let Some(_) = task_pool.next().await {} }.boxed()
}

fn open_s3_files(work_dir: &WorkDir, num_chunks: usize, args: &Args) -> Vec<ObjectStoreFile> {
    (0..num_chunks)
        .map(|chunk_index| {
            let path = parq_file_path(
                work_dir,
                args.row_group_size,
                args.page_size_kb,
                chunk_index,
                args.data_type,
                args.compression,
                args.dictionary,
            );
            work_dir.s3_file(path)
        })
        .collect::<Vec<_>>()
}

fn open_local_files(work_dir: &WorkDir, num_chunks: usize, args: &Args) -> Vec<ReadAtFile> {
    (0..num_chunks)
        .map(|chunk_index| {
            let path = parq_file_path(
                work_dir,
                args.row_group_size,
                args.page_size_kb,
                chunk_index,
                args.data_type,
                args.compression,
                args.dictionary,
            );
            work_dir.local_file(path)
        })
        .collect::<Vec<_>>()
}

async fn run_bench_parquet<T: FileLike>(
    args: &Args,
    indices: &RandomIndices,
    files: &[T],
    metadata_lookup: &[Option<ArrowReaderMetadata>],
) -> f64 {
    if args.drop_caches {
        drop_caches();
    }

    let rt = Arc::new(tokio::runtime::Builder::new_multi_thread().build().unwrap());

    let start = Instant::now();

    log("Running benchmark");
    let iterations = if args.duration_seconds == 0.0 {
        // Special case, run once
        bench_parquet_one(args, &indices, &files, &metadata_lookup).await;
        1
    } else {
        let tasks = std::iter::repeat(())
            .map(|_| {
                let task = bench_parquet_one(&args, &indices, &files, &metadata_lookup);
                tokio::task::spawn(task).map(|res| res.unwrap())
            })
            .take_while(|_| start.elapsed().as_secs_f64() < args.duration_seconds);
        let stream = futures::stream::iter(tasks);
        if args.concurrency == 0 {
            panic!("Invalid concurrency");
        }
        stream.buffer_unordered(args.concurrency).count().await
    };

    log(format!("Iterations = {}", iterations));
    log(format!("Take size = {}", args.take_size));
    log(format!(
        "Duration = {} seconds",
        start.elapsed().as_secs_f64()
    ));
    let rows_taken = TAKE_COUNTER.load(Ordering::Acquire) as f64;
    let rows_taken_per_second = rows_taken / start.elapsed().as_secs_f64();

    Arc::into_inner(rt).unwrap().shutdown_background();

    rows_taken_per_second
}

async fn bench_parquet(args: &Args, work_dir: &WorkDir) -> f64 {
    parquet_global_setup(
        args.row_group_size,
        args.page_size_kb,
        args.rows_per_file,
        args.num_files,
        args.data_type,
        work_dir,
        args.compression,
        args.dictionary,
    )
    .await;

    log("Randomizing indices");
    let indices = RandomIndices::new(args.rows_per_file, args.num_files).await;

    log("Loading metadata");
    let args_copy = args.clone();
    let work_dir_copy = work_dir.clone();
    let metadata_lookup =
        tokio::task::spawn_blocking(move || parquet_setup_sync(&args_copy, &work_dir_copy))
            .await
            .unwrap();

    // Don't set LOG_READS until this point to avoid logging the setup
    if args.log_reads {
        LOG_READS.store(true, std::sync::atomic::Ordering::Release);
    }

    log("Opening files");
    if work_dir.is_s3() {
        let files = open_s3_files(work_dir, args.num_files, args);
        run_bench_parquet(args, &indices, &files, &metadata_lookup).await
    } else {
        let files = open_local_files(work_dir, args.num_files, args);
        run_bench_parquet(args, &indices, &files, &metadata_lookup).await
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let _guard = if args.tracing {
        let (chrome_layer, guard) = ChromeLayerBuilder::new().build();
        tracing_subscriber::registry()
            .with(chrome_layer)
            .with(LevelFilter::from_level(Level::DEBUG))
            .init();
        Some(guard)
    } else {
        None
    };

    if !args.quiet {
        SHOULD_LOG.store(true, std::sync::atomic::Ordering::Release);
    }

    if args.rust_logging {
        env_logger::init();
    }

    let workdir = args
        .workdir
        .as_ref()
        .map(|s| s.as_str())
        .unwrap_or("file:///tmp")
        .to_string();

    let work_dir = WorkDir::new(&workdir).await;

    let rows_taken_per_second = bench_parquet(&args, &work_dir).await;

    log(format!(
        "Rows taken per second across {} seconds: {}",
        args.duration_seconds, rows_taken_per_second,
    ));
    if args.quiet {
        println!("{}", rows_taken_per_second);
    }
}
