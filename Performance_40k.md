echo -e "40000\n2" | uv run python src/performance_comparison.py
🔥 API Performance Testing Comparison Tool
============================================================
✅ API Status: {'status': 'healthy', 'json_parser': 'orjson', 'project_manager': 'uv', 'server': 'Flask', 'threading': 'enabled', 'timestamp': 1762140946.706584}

Available sample sizes: [50, 100, 200]
Enter sample size (or custom number): 📁 Loading data from data/data.txt...
✅ Loaded 40000 lines

🧪 Testing with 40000 lines

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

Progress: 1000/40000 lines processed | Avg API time: 1975.67ms | Avg JSON parse: 0.0157ms
Progress: 2000/40000 lines processed | Avg API time: 1983.79ms | Avg JSON parse: 0.0149ms
Progress: 3000/40000 lines processed | Avg API time: 1992.18ms | Avg JSON parse: 0.0146ms
Progress: 4000/40000 lines processed | Avg API time: 2001.69ms | Avg JSON parse: 0.0144ms
Progress: 5000/40000 lines processed | Avg API time: 2000.54ms | Avg JSON parse: 0.0144ms
Progress: 6000/40000 lines processed | Avg API time: 1999.91ms | Avg JSON parse: 0.0144ms
Progress: 7000/40000 lines processed | Avg API time: 2002.97ms | Avg JSON parse: 0.0143ms
Progress: 8000/40000 lines processed | Avg API time: 2004.52ms | Avg JSON parse: 0.0143ms
Progress: 9000/40000 lines processed | Avg API time: 2006.13ms | Avg JSON parse: 0.0143ms
Progress: 10000/40000 lines processed | Avg API time: 2006.58ms | Avg JSON parse: 0.0143ms
Progress: 11000/40000 lines processed | Avg API time: 2007.09ms | Avg JSON parse: 0.0142ms
Progress: 12000/40000 lines processed | Avg API time: 2009.01ms | Avg JSON parse: 0.0142ms
Progress: 13000/40000 lines processed | Avg API time: 2012.63ms | Avg JSON parse: 0.0142ms
Progress: 14000/40000 lines processed | Avg API time: 2011.18ms | Avg JSON parse: 0.0142ms
Progress: 15000/40000 lines processed | Avg API time: 2007.40ms | Avg JSON parse: 0.0142ms
Progress: 16000/40000 lines processed | Avg API time: 2006.89ms | Avg JSON parse: 0.0142ms
Progress: 17000/40000 lines processed | Avg API time: 2008.20ms | Avg JSON parse: 0.0142ms
Progress: 18000/40000 lines processed | Avg API time: 2009.83ms | Avg JSON parse: 0.0142ms
Progress: 19000/40000 lines processed | Avg API time: 2009.10ms | Avg JSON parse: 0.0142ms
Progress: 20000/40000 lines processed | Avg API time: 2006.54ms | Avg JSON parse: 0.0142ms
Progress: 21000/40000 lines processed | Avg API time: 2007.43ms | Avg JSON parse: 0.0142ms
Progress: 22000/40000 lines processed | Avg API time: 2006.81ms | Avg JSON parse: 0.0142ms
Progress: 23000/40000 lines processed | Avg API time: 2008.59ms | Avg JSON parse: 0.0142ms
Progress: 24000/40000 lines processed | Avg API time: 2009.61ms | Avg JSON parse: 0.0142ms
Progress: 25000/40000 lines processed | Avg API time: 2009.01ms | Avg JSON parse: 0.0142ms
Progress: 26000/40000 lines processed | Avg API time: 2009.09ms | Avg JSON parse: 0.0142ms
Progress: 27000/40000 lines processed | Avg API time: 2010.40ms | Avg JSON parse: 0.0142ms
Progress: 28000/40000 lines processed | Avg API time: 2009.91ms | Avg JSON parse: 0.0142ms
Progress: 29000/40000 lines processed | Avg API time: 2010.71ms | Avg JSON parse: 0.0142ms
Progress: 30000/40000 lines processed | Avg API time: 2010.84ms | Avg JSON parse: 0.0142ms
Progress: 31000/40000 lines processed | Avg API time: 2012.64ms | Avg JSON parse: 0.0142ms
Progress: 32000/40000 lines processed | Avg API time: 2012.84ms | Avg JSON parse: 0.0142ms
Progress: 33000/40000 lines processed | Avg API time: 2013.35ms | Avg JSON parse: 0.0142ms
Progress: 34000/40000 lines processed | Avg API time: 2012.69ms | Avg JSON parse: 0.0142ms
Progress: 35000/40000 lines processed | Avg API time: 2012.71ms | Avg JSON parse: 0.0142ms
Progress: 36000/40000 lines processed | Avg API time: 2013.46ms | Avg JSON parse: 0.0142ms
Progress: 37000/40000 lines processed | Avg API time: 2014.05ms | Avg JSON parse: 0.0142ms
Progress: 38000/40000 lines processed | Avg API time: 2014.00ms | Avg JSON parse: 0.0142ms
Progress: 39000/40000 lines processed | Avg API time: 2014.04ms | Avg JSON parse: 0.0142ms
Progress: 40000/40000 lines processed | Avg API time: 2014.62ms | Avg JSON parse: 0.0142ms

============================================================
Processing Complete!
============================================================
Total lines processed: 40000
Total time: 4044.86 seconds (67.41 minutes)
Average time per line: 101.12 ms
Throughput: 9.89 lines/second
Total API calls made: 80000

API Response Time Statistics (both calls combined):
  Min: 30.60 ms
  Max: 3976.78 ms
  Mean: 2014.62 ms
  Median: 2010.59 ms
  Std Dev: 812.74 ms

JSON Parsing Time Statistics (orjson, both calls combined):
  Min: 0.0045 ms
  Max: 0.1300 ms
  Mean: 0.0142 ms
  Median: 0.0135 ms
  Total: 568.86 ms (0.57 seconds)
  % of total time: 0.014%
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

Created 400 partitions with ~100 rows each
Computing results...

============================================================
Processing Complete!
============================================================
Total lines processed: 40000
Total time: 5108.33 seconds (85.14 minutes)
Average time per line: 127.71 ms
Throughput: 7.83 lines/second
Total API calls made: 80000
Partitions processed: 400

API Response Time Statistics (both calls combined):
  Min: 33.29 ms
  Max: 3988.90 ms
  Mean: 2015.82 ms
  Median: 2016.52 ms
  Std Dev: 811.32 ms

JSON Parsing Time Statistics (orjson, both calls combined):
  Min: 0.0051 ms
  Max: 0.6737 ms
  Mean: 0.0157 ms
  Median: 0.0148 ms
  Total: 628.99 ms (0.63 seconds)
  % of total time: 0.012%
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

Created 1600 batches of ~25 lines each
Computing results...

============================================================
Processing Complete!
============================================================
Total lines processed: 40000
Total time: 5057.91 seconds (84.30 minutes)
Average time per line: 126.45 ms
Throughput: 7.91 lines/second
Total API calls made: 80000
Batches processed: 1600

API Response Time Statistics (both calls combined):
  Min: 47.78 ms
  Max: 3995.55 ms
  Mean: 2009.44 ms
  Median: 2004.21 ms
  Std Dev: 813.32 ms

JSON Parsing Time Statistics (orjson, both calls combined):
  Min: 0.0049 ms
  Max: 0.4020 ms
  Mean: 0.0161 ms
  Median: 0.0152 ms
  Total: 645.30 ms (0.65 seconds)
  % of total time: 0.013%
============================================================


================================================================================
📊 PERFORMANCE COMPARISON RESULTS (40000 lines)
================================================================================
Method               Time (s)     Throughput      Speedup    Status
-------------------- ------------ --------------- ---------- ----------
Multi-threaded       4044.92      9.89            1.26      x ✅ Success
Dask Delayed         5057.96      7.91            1.01      x ✅ Success
Dask DataFrame       5108.38      7.83            1.00      x ✅ Success

🏆 WINNER: Multi-threaded
   ⏱️  Time: 4044.92 seconds
   🚀 Throughput: 9.89 lines/second
   📈 Improvement: 1.26x faster than slowest method

✨ Comparison complete!