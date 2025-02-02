# Random point lookup benchmark

This is a simple benchmark to measure the number of point lookups we can perform on
a parquet dataset with 2 million vectors.

Indices are chosen randomly, without repeats, for 10 seconds. After 10 seconds we
report the average number of take operations.

The results were gathered with the following command:

```bash
cargo run --release -- --workdir s3://bucket/path --take-size 1 --num-files 10 --cache-metadata --concurrency 256 --duration-seconds 10 --quiet --page-size-kb 8
```

We captured results on a c5d instance communicating with a regular S3 bucket.

The baseline numbers were obtained using s3prof.rs, a simple tool that measures the
number of IOPS we can perform, ignoring any kind of file format. To run this tool
simply do (you will need to modify the source code to change the bucket name):

```bash
cargo run --release --bin s3prof
```

The results will be placed in a file named s3_results.csv
