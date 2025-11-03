 echo -e "1000\n2" | uv run python src/performance_comparison.py
🔥 API Performance Testing Comparison Tool
============================================================
✅ API Status: {'status': 'healthy', 'json_parser': 'orjson', 'project_manager': 'uv', 'server': 'Flask', 'threading': 'enabled', 'timestamp': 1762139662.4732754}

Available sample sizes: [50, 100, 200]
Enter sample size (or custom number): 📁 Loading data from data/data.txt...
✅ Loaded 1000 lines

🧪 Testing with 1000 lines

Which methods would you like to test?
1. All methods (recommended)
2. Only fast methods (Multi-threaded + Dask)
3. Custom selection
Enter choice (1/2/3): 
============================================================
🚀 MULTI-THREADED PROCESSING TEST
============================================================

============================================================
Starting multithreaded processing with 20 workers
Method: ThreadPoolExecutor with DataFrame.apply pattern
HTTP Client: httpx with connection pooling
JSON Parser: orjson (high-performance)
Project Manager: uv (ultra-fast)
API Latency: 10ms - 2000ms per call (random)
Expected range: 20ms - 4000ms per line (2 API calls)
============================================================

2
Progress: 1000/1000 lines processed | Avg API time: 2029.20ms | Avg JSON parse: 0.0168ms

============================================================
Processing Complete!
============================================================
Total lines processed: 1000
Total time: 103.38 seconds (1.72 minutes)
Average time per line: 103.38 ms
Throughput: 9.67 lines/second
Total API calls made: 2000

API Response Time Statistics (both calls combined):
  Min: 121.52 ms
  Max: 3902.33 ms
  Mean: 2029.20 ms
  Median: 2004.18 ms
  Std Dev: 810.44 ms

JSON Parsing Time Statistics (orjson, both calls combined):
  Min: 0.0049 ms
  Max: 0.0611 ms
  Mean: 0.0168 ms
  Median: 0.0158 ms
  Total: 16.76 ms (0.02 seconds)
  % of total time: 0.016%
============================================================


============================================================
⚡ DASK DATAFRAME PROCESSING TEST
============================================================

============================================================
Starting Dask distributed processing
Method: Dask DataFrame with custom partitions
HTTP Client: httpx with connection pooling
JSON Parser: orjson (high-performance)
Project Manager: uv (ultra-fast)
API Latency: 10ms - 2000ms per call (random)
Expected range: 20ms - 4000ms per line (2 API calls)
============================================================

Created 10 partitions with ~100 rows each
Computing results...

============================================================
Processing Complete!
============================================================
Total lines processed: 1000
Total time: 212.87 seconds (3.55 minutes)
Average time per line: 212.87 ms
Throughput: 4.70 lines/second
Total API calls made: 2000
Partitions processed: 10

API Response Time Statistics (both calls combined):
  Min: 144.90 ms
  Max: 3928.56 ms
  Mean: 2031.16 ms
  Median: 2043.56 ms
  Std Dev: 813.67 ms

JSON Parsing Time Statistics (orjson, both calls combined):
  Min: 0.0063 ms
  Max: 0.1035 ms
  Mean: 0.0170 ms
  Median: 0.0162 ms
  Total: 17.04 ms (0.02 seconds)
  % of total time: 0.008%
============================================================


============================================================
⚡ DASK DELAYED PROCESSING TEST
============================================================

============================================================
Starting Dask delayed processing with batch size 25
Method: Dask delayed with custom batching
HTTP Client: httpx with connection pooling
JSON Parser: orjson (high-performance)
Project Manager: uv (ultra-fast)
API Latency: 10ms - 2000ms per call (random)
Expected range: 20ms - 4000ms per line (2 API calls)
============================================================

Created 40 batches of ~25 lines each
Computing results...

============================================================
Processing Complete!
============================================================
Total lines processed: 1000
Total time: 153.28 seconds (2.55 minutes)
Average time per line: 153.28 ms
Throughput: 6.52 lines/second
Total API calls made: 2000
Batches processed: 40

API Response Time Statistics (both calls combined):
  Min: 163.10 ms
  Max: 3969.92 ms
  Mean: 2034.09 ms
  Median: 2021.13 ms
  Std Dev: 813.45 ms

JSON Parsing Time Statistics (orjson, both calls combined):
  Min: 0.0067 ms
  Max: 0.0716 ms
  Mean: 0.0173 ms
  Median: 0.0161 ms
  Total: 17.33 ms (0.02 seconds)
  % of total time: 0.011%
============================================================


================================================================================
📊 PERFORMANCE COMPARISON RESULTS (1000 lines)
================================================================================
Method               Time (s)     Throughput      Speedup    Status
-------------------- ------------ --------------- ---------- ----------
Multi-threaded       103.39       9.67            2.06      x ✅ Success
Dask Delayed         153.28       6.52            1.39      x ✅ Success
Dask DataFrame       212.87       4.70            1.00      x ✅ Success

🏆 WINNER: Multi-threaded
   ⏱️  Time: 103.39 seconds
   🚀 Throughput: 9.67 lines/second
   📈 Improvement: 2.06x faster than slowest method

✨ Comparison complete!