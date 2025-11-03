# 🚀 Parallel Processing Performance Benchmark

A comprehensive performance benchmarking framework comparing different parallel processing approaches for data processing workflows that combine file processing with API calls. This project demonstrates the performance differences between sequential, multi-threaded, and Dask-based processing for typical data pipeline scenarios.

## 🎯 Project Overview

This framework benchmarks parallel processing performance using realistic data pipeline scenarios with:
- **File processing** (CSV/text file parsing)
- **API integration** with random latency (10ms - 2000ms per call)
- **High-performance JSON parsing** with orjson
- **Connection pooling** with httpx
- **Multiple processing strategies** for comparison

## 🏗️ Architecture

```
├── src/
│   ├── api_server.py              # Flask API server with simulated latency
│   ├── process_data.py            # Multi-threaded data processing implementation
│   ├── process_data_dask.py       # Dask-based data processing implementation
│   └── performance_comparison.py  # Comprehensive performance comparison tool
├── data/
│   ├── data.txt                   # Test dataset (40,000 lines of pipe-separated data)
│   └── processed_data*.csv        # Results from data processing workflows
└── tests/
    └── __init__.py
```

## 🛠️ Tech Stack

- **Language**: Python 3.10+
- **Package Manager**: uv (ultra-fast Python package installer)
- **API Framework**: Flask with orjson for high-performance JSON
- **HTTP Client**: httpx with HTTP/2 support and connection pooling
- **File Processing**: pandas for structured data handling
- **Parallel Processing Approaches**: 
  - ThreadPoolExecutor (built-in concurrency)
  - Dask (distributed computing framework)
- **Data Formats**: CSV, pipe-separated text files
- **Linting**: ruff

## ⚡ Quick Start

### 1. Setup Environment

```bash
# Clone the repository
git clone <repository-url>
cd parallel-processing-benchmark

# Install dependencies with uv
uv sync
```

### 2. Start API Server

```bash
# Terminal 1: Start the Flask API server
uv run python src/api_server.py
```

### 3. Run Performance Benchmarks

```bash
# Terminal 2: Run individual benchmarks
uv run python src/process_data.py          # Multi-threaded approach
uv run python src/process_data_dask.py     # Dask approach

# Or run comprehensive performance comparison
uv run python src/performance_comparison.py
```

## 📊 Performance Benchmark Results

### 🏆 Data Processing Pipeline Results (50 Lines)

| Rank | Processing Method | Time (s) | Throughput (lines/sec) | Speedup | Status |
|------|-------------------|----------|------------------------|---------|--------|
| 🥇 | **Multi-threaded** | **7.31** | **6.84** | **13.62x** | ✅ Winner |
| 🥈 | Dask Delayed | 57.84 | 0.86 | 1.72x | ✅ Good |
| 🥉 | Dask DataFrame | 99.57 | 0.50 | 1.00x | ✅ Slowest |

### 📈 Key Performance Insights

#### 🚀 **Multi-threaded Approach DOMINATES**
- **13.6x faster** than Dask DataFrame for data pipeline workloads
- **Perfect for I/O-bound operations** like file processing + API calls
- **Minimal overhead** with maximum efficiency
- **httpx connection pooling** optimizes concurrent HTTP requests

#### ⚡ **Dask Performance Characteristics**
- **High overhead** for small to medium datasets
- **Better suited** for large-scale distributed data processing
- **Dask Delayed > Dask DataFrame** for this type of workflow
- **Significant task scheduling overhead** for smaller workloads

## 🔧 API Endpoints

The Flask server provides these endpoints:

| Endpoint | Method | Description | Response Time |
|----------|--------|-------------|---------------|
| `/health` | GET | Health check | Instant |
| `/capitalize` | POST | Capitalize first word | 10ms - 2000ms |
| `/line_length` | POST | Calculate line length | 10ms - 2000ms |

### Example API Usage

```bash
# Health check
curl -X GET http://localhost:5000/health

# Capitalize endpoint
curl -X POST http://localhost:5000/capitalize \
  -H "Content-Type: application/json" \
  -d '{"line":"hello|world|123|test"}'

# Length endpoint  
curl -X POST http://localhost:5000/line_length \
  -H "Content-Type: application/json" \
  -d '{"line":"hello|world|123|test"}'
```

## 🎯 Use Cases & Recommendations

### 🏅 **When to Use Multi-threaded**
- ✅ Small to medium datasets (< 10K records)
- ✅ Data pipelines with I/O operations (file reading + API calls)
- ✅ Single machine deployments
- ✅ Need maximum performance with minimal overhead
- ✅ ETL workflows with external service integration

### ⚙️ **When to Use Dask**
- ✅ Large datasets (> 100K records)
- ✅ Distributed computing requirements
- ✅ Complex data transformations and aggregations
- ✅ Multi-machine clusters
- ✅ Memory-intensive data processing

### 📊 **Data Processing Pipeline Scaling Characteristics**

```
Dataset Size    | Multi-threaded | Dask Delayed | Dask DataFrame | Use Case
----------------|----------------|--------------|----------------|----------
< 1K lines      | 🚀 Excellent   | ⚠️ Overhead   | ❌ Poor        | Prototypes, small ETL
1K - 10K lines  | 🚀 Excellent   | ✅ Good       | ⚠️ Moderate    | Medium ETL, data enrichment  
10K - 100K lines| ✅ Good        | 🚀 Excellent  | ✅ Good        | Large ETL, batch processing
> 100K lines    | ⚠️ Memory      | 🚀 Excellent  | 🚀 Excellent   | Big data, distributed processing
```

## 🔍 Code Quality

This project maintains high code quality standards:

```bash
# Lint code with ruff
uv run ruff check src/

# Auto-fix issues
uv run ruff check src/ --fix

# All checks currently pass! ✅
```

## 🚀 Features

### ⚡ **High Performance Data Processing**
- **HTTP/2 support** with httpx
- **Connection pooling** for efficient resource usage
- **orjson** for ultra-fast JSON parsing
- **Concurrent processing** with ThreadPoolExecutor
- **Pandas integration** for structured data handling

### 📊 **Comprehensive Performance Monitoring**
- **Real-time progress tracking** (every 1000 processed records)
- **Detailed timing statistics** (file I/O, API calls, JSON parsing)
- **Throughput measurements** (records/second)
- **Performance comparisons** across processing methods

### 🛡️ **Robust Data Pipeline Features**
- **Connection retry logic** with exponential backoff
- **Graceful failure handling** for individual processing steps
- **Comprehensive logging** for debugging data pipelines
- **Health checks** before processing workflows

## 📝 Configuration

Key configuration options in the code:

```python
# API Configuration
API_BASE_URL = "http://localhost:5000"
MAX_WORKERS = 20                    # Thread pool size
CHUNK_SIZE = 100                    # Dask partition size

# Performance Tuning
TIMEOUT = 10.0                      # HTTP timeout
MAX_CONNECTIONS = 100               # Connection pool size
MAX_KEEPALIVE = 20                  # Keep-alive connections
```

## 🧪 Testing

The project includes comprehensive testing capabilities:

```bash
# Test with different sample sizes
echo "50" | uv run python src/performance_comparison.py
echo "100" | uv run python src/performance_comparison.py
echo "200" | uv run python src/performance_comparison.py

# Test individual approaches
uv run python src/process_data.py
uv run python src/process_data_dask.py
```

## 📚 Dependencies

Core dependencies managed by uv:

```toml
[project.dependencies]
flask = ">=3.0.0"          # Web framework
pandas = ">=2.1.4"         # Data processing
httpx = ">=0.27.0"         # HTTP client with HTTP/2
orjson = ">=3.9.10"        # Fast JSON parsing
dask = ">=2025.10.0"       # Distributed computing

[project.optional-dependencies.dev]
pytest = ">=7.4.0"         # Testing framework
ruff = ">=0.14.3"          # Linting and formatting
```

## 🎨 Sample Output

```
🔥 Parallel Processing Performance Benchmark
============================================================
✅ API Status: {'status': 'healthy', 'json_parser': 'orjson'}

🧪 Benchmarking with 50 lines

🚀 MULTI-THREADED DATA PROCESSING
============================================================
Progress: 50/50 lines processed | Avg API time: 2144.63ms
Total time: 7.31 seconds | Throughput: 6.84 lines/second

🏆 WINNER: Multi-threaded
   ⏱️  Time: 7.31 seconds
   🚀 Throughput: 6.84 lines/second
   📈 Improvement: 13.62x faster than slowest method
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Run linting (`uv run ruff check src/`)
4. Commit changes (`git commit -m 'Add amazing feature'`)
5. Push to branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

**Key License Points:**
- ✅ **Commercial use** allowed
- ✅ **Modification** allowed  
- ✅ **Distribution** allowed
- ✅ **Patent use** allowed
- ⚠️ **Trademark use** not allowed
- 📋 **License and copyright notice** required

## 🙏 Acknowledgments

- **orjson** for ultra-fast JSON processing
- **httpx** for modern HTTP client capabilities
- **Dask** for distributed computing framework
- **uv** for ultra-fast Python package management
- **ruff** for lightning-fast Python linting

---

**Built with ❤️ and ⚡ by the Parallel Processing Benchmark Team**

*"Benchmarking performance, one data pipeline at a time!"* 🚀