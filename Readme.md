# SF Crime Statistics with Spark Streaming

1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

Increasing _maxOffsetsPerTrigger_ and _spark.default.parallelism_ increased throughput but also increased latency.

2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

By setting _maxOffsetsPerTrigger_ to _800_ and _spark.default.parallelism_ to _100_ I was able to maximize the _processedRowsPerSecond_.