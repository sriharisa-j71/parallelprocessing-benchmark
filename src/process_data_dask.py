import dask.dataframe as dd
import pandas as pd
import httpx
import time
import sys
import statistics
import orjson
from pathlib import Path
from functools import partial
from typing import Optional
import random
import string
from dask.distributed import Client, as_completed
from dask import delayed, compute
import warnings

# Configuration
API_BASE_URL = "http://localhost:5000"
CHUNK_SIZE = 100  # Number of rows per partition

# Suppress Dask warnings for cleaner output
warnings.filterwarnings("ignore", category=UserWarning, module="dask")


class DaskAPIClient:
    """HTTP client for API calls with connection pooling"""

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.client = httpx.Client(
            base_url=base_url,
            timeout=httpx.Timeout(10.0),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            transport=httpx.HTTPTransport(retries=3),
            http2=True,  # Enable HTTP/2 for better performance
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def call_capitalize_api(self, line: str) -> tuple[Optional[str], float, float]:
        """
        Make API call to capitalize endpoint
        Returns: (capitalized_text, api_time_ms, json_parse_time_ms)
        """
        try:
            # Time the API call
            start_time = time.perf_counter()
            response = self.client.post(
                "/capitalize", content=orjson.dumps({"line": line})
            )
            api_time = (time.perf_counter() - start_time) * 1000  # Convert to ms

            response.raise_for_status()

            # Time the JSON parsing
            parse_start = time.perf_counter()
            data = orjson.loads(response.content)
            parse_time = (time.perf_counter() - parse_start) * 1000  # Convert to ms

            return data.get("capitalized"), api_time, parse_time

        except Exception as e:
            print(f"API call failed for line: {line[:50]}... Error: {e}")
            return None, 0, 0

    def call_length_api(self, line: str) -> tuple[Optional[int], float, float]:
        """
        Make API call to line length endpoint
        Returns: (length, api_time_ms, json_parse_time_ms)
        """
        try:
            # Time the API call
            start_time = time.perf_counter()
            response = self.client.post(
                "/line_length", content=orjson.dumps({"line": line})
            )
            api_time = (time.perf_counter() - start_time) * 1000  # Convert to ms

            response.raise_for_status()

            # Time the JSON parsing
            parse_start = time.perf_counter()
            data = orjson.loads(response.content)
            parse_time = (time.perf_counter() - parse_start) * 1000  # Convert to ms

            return data.get("length"), api_time, parse_time

        except Exception as e:
            print(f"API call failed for line: {line[:50]}... Error: {e}")
            return None, 0, 0

    def health_check(self):
        """Check API health"""
        try:
            response = self.client.get("/health")
            response.raise_for_status()
            return orjson.loads(response.content)
        except Exception as e:
            raise ConnectionError(f"Cannot connect to API: {e}") from e


def process_single_line_dask(line: str, api_client: DaskAPIClient) -> dict:
    """
    Process a single line using API calls - Dask compatible function
    Returns dict with all results
    """
    # Call both APIs
    capitalized, api_time1, parse_time1 = api_client.call_capitalize_api(line)
    length, api_time2, parse_time2 = api_client.call_length_api(line)

    return {
        "line": line,
        "capitalized": capitalized or "",
        "length": length or 0,
        "api_time_ms": api_time1 + api_time2,
        "json_parse_time_ms": parse_time1 + parse_time2,
    }


def process_partition_dask(partition_df: pd.DataFrame, base_url: str) -> pd.DataFrame:
    """
    Process a partition of the dataframe using Dask
    This function will be applied to each partition
    """
    results = []

    with DaskAPIClient(base_url) as api_client:
        for _, row in partition_df.iterrows():
            line = row["line"]
            result = process_single_line_dask(line, api_client)
            results.append(result)

    return pd.DataFrame(results)


def process_dataframe_dask(df: pd.DataFrame, base_url: str = API_BASE_URL) -> pd.DataFrame:
    """Process dataframe using Dask distributed computing"""
    print(f"\n{'='*60}")
    print(f"Starting Dask distributed processing")
    print("Method: Dask DataFrame with custom partitions")
    print("HTTP Client: httpx with connection pooling")
    print("JSON Parser: orjson (high-performance)")
    print("Project Manager: uv (ultra-fast)")
    print("API Latency: 10ms - 2000ms per call (random)")
    print("Expected range: 20ms - 4000ms per line (2 API calls)")
    print(f"{'='*60}\n")

    start_time = time.time()

    # Convert to Dask DataFrame with specified chunk size
    ddf = dd.from_pandas(df, chunksize=CHUNK_SIZE)

    print(f"Created {ddf.npartitions} partitions with ~{CHUNK_SIZE} rows each")

    # Apply the processing function to each partition
    processed_ddf = ddf.map_partitions(
        process_partition_dask, base_url, meta=pd.DataFrame(columns=[
            "line", "capitalized", "length", "api_time_ms", "json_parse_time_ms"
        ])
    )

    # Compute the result (this triggers the actual computation)
    print("Computing results...")
    final_df = processed_ddf.compute()

    elapsed_time = time.time() - start_time

    # Calculate statistics
    api_times = final_df["api_time_ms"].tolist()
    parse_times = final_df["json_parse_time_ms"].tolist()

    api_stats = {}
    parse_stats = {}

    if api_times:
        api_stats = {
            "min": min(api_times),
            "max": max(api_times),
            "mean": statistics.mean(api_times),
            "median": statistics.median(api_times),
            "stdev": statistics.stdev(api_times) if len(api_times) > 1 else 0,
        }

    if parse_times:
        parse_stats = {
            "min": min(parse_times),
            "max": max(parse_times),
            "mean": statistics.mean(parse_times),
            "median": statistics.median(parse_times),
            "total": sum(parse_times),
        }

    # Print statistics
    print(f"\n{'='*60}")
    print("Processing Complete!")
    print(f"{'='*60}")
    print(f"Total lines processed: {len(final_df)}")
    print(f"Total time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    print(f"Average time per line: {elapsed_time*1000/len(final_df):.2f} ms")
    print(f"Throughput: {len(final_df)/elapsed_time:.2f} lines/second")
    print(f"Total API calls made: {len(final_df) * 2}")
    print(f"Partitions processed: {ddf.npartitions}")

    print("\nAPI Response Time Statistics (both calls combined):")
    if api_stats:
        print(f"  Min: {api_stats['min']:.2f} ms")
        print(f"  Max: {api_stats['max']:.2f} ms")
        print(f"  Mean: {api_stats['mean']:.2f} ms")
        print(f"  Median: {api_stats['median']:.2f} ms")
        print(f"  Std Dev: {api_stats['stdev']:.2f} ms")

    print("\nJSON Parsing Time Statistics (orjson, both calls combined):")
    if parse_stats:
        print(f"  Min: {parse_stats['min']:.4f} ms")
        print(f"  Max: {parse_stats['max']:.4f} ms")
        print(f"  Mean: {parse_stats['mean']:.4f} ms")
        print(f"  Median: {parse_stats['median']:.4f} ms")
        print(f"  Total: {parse_stats['total']:.2f} ms ({parse_stats['total']/1000:.2f} seconds)")
        print(f"  % of total time: {(parse_stats['total']/1000)/elapsed_time*100:.3f}%")

    print(f"{'='*60}\n")

    return final_df


@delayed
def process_batch_delayed(lines_batch: list, base_url: str) -> list:
    """
    Process a batch of lines using Dask delayed
    """
    results = []
    with DaskAPIClient(base_url) as api_client:
        for line in lines_batch:
            result = process_single_line_dask(line, api_client)
            results.append(result)
    return results


def process_with_dask_delayed(df: pd.DataFrame, batch_size: int = 50, base_url: str = API_BASE_URL) -> pd.DataFrame:
    """Process dataframe using Dask delayed with custom batching"""
    print(f"\n{'='*60}")
    print(f"Starting Dask delayed processing with batch size {batch_size}")
    print("Method: Dask delayed with custom batching")
    print("HTTP Client: httpx with connection pooling")
    print("JSON Parser: orjson (high-performance)")
    print("Project Manager: uv (ultra-fast)")
    print("API Latency: 10ms - 2000ms per call (random)")
    print("Expected range: 20ms - 4000ms per line (2 API calls)")
    print(f"{'='*60}\n")

    start_time = time.time()

    # Split data into batches
    lines = df["line"].tolist()
    batches = [lines[i:i + batch_size] for i in range(0, len(lines), batch_size)]

    print(f"Created {len(batches)} batches of ~{batch_size} lines each")

    # Create delayed computations
    delayed_results = [process_batch_delayed(batch, base_url) for batch in batches]

    # Compute all batches in parallel
    print("Computing results...")
    batch_results = compute(*delayed_results)

    # Flatten results
    all_results = []
    for batch_result in batch_results:
        all_results.extend(batch_result)

    final_df = pd.DataFrame(all_results)
    elapsed_time = time.time() - start_time

    # Calculate statistics (same as above)
    api_times = final_df["api_time_ms"].tolist()
    parse_times = final_df["json_parse_time_ms"].tolist()

    api_stats = {}
    parse_stats = {}

    if api_times:
        api_stats = {
            "min": min(api_times),
            "max": max(api_times),
            "mean": statistics.mean(api_times),
            "median": statistics.median(api_times),
            "stdev": statistics.stdev(api_times) if len(api_times) > 1 else 0,
        }

    if parse_times:
        parse_stats = {
            "min": min(parse_times),
            "max": max(parse_times),
            "mean": statistics.mean(parse_times),
            "median": statistics.median(parse_times),
            "total": sum(parse_times),
        }

    # Print statistics
    print(f"\n{'='*60}")
    print("Processing Complete!")
    print(f"{'='*60}")
    print(f"Total lines processed: {len(final_df)}")
    print(f"Total time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    print(f"Average time per line: {elapsed_time*1000/len(final_df):.2f} ms")
    print(f"Throughput: {len(final_df)/elapsed_time:.2f} lines/second")
    print(f"Total API calls made: {len(final_df) * 2}")
    print(f"Batches processed: {len(batches)}")

    print("\nAPI Response Time Statistics (both calls combined):")
    if api_stats:
        print(f"  Min: {api_stats['min']:.2f} ms")
        print(f"  Max: {api_stats['max']:.2f} ms")
        print(f"  Mean: {api_stats['mean']:.2f} ms")
        print(f"  Median: {api_stats['median']:.2f} ms")
        print(f"  Std Dev: {api_stats['stdev']:.2f} ms")

    print("\nJSON Parsing Time Statistics (orjson, both calls combined):")
    if parse_stats:
        print(f"  Min: {parse_stats['min']:.4f} ms")
        print(f"  Max: {parse_stats['max']:.4f} ms")
        print(f"  Mean: {parse_stats['mean']:.4f} ms")
        print(f"  Median: {parse_stats['median']:.4f} ms")
        print(f"  Total: {parse_stats['total']:.2f} ms ({parse_stats['total']/1000:.2f} seconds)")
        print(f"  % of total time: {(parse_stats['total']/1000)/elapsed_time*100:.3f}%")

    print(f"{'='*60}\n")

    return final_df


def create_sample_data(filename="data/data.txt", lines=40000):
    """Create a sample pipe-separated file for testing"""
    print(f"Creating sample data file with {lines} lines...")

    # Ensure data directory exists
    Path(filename).parent.mkdir(parents=True, exist_ok=True)

    with open(filename, "w") as f:
        for _i in range(lines):
            # Generate random pipe-separated data
            word1 = "".join(random.choices(string.ascii_lowercase, k=random.randint(5, 15)))
            word2 = "".join(random.choices(string.ascii_lowercase, k=random.randint(5, 15)))
            word3 = "".join(random.choices(string.digits, k=random.randint(3, 10)))
            word4 = "".join(random.choices(string.ascii_lowercase, k=random.randint(5, 20)))

            f.write(f"{word1}|{word2}|{word3}|{word4}\n")

    print(f"Sample data created: {filename}")


def main():
    """Main function to run the Dask-based API performance test"""
    try:
        # Test API connection
        with DaskAPIClient(API_BASE_URL) as api_client:
            health_data = api_client.health_check()
            print(f"API Status: {health_data}")
    except ConnectionError as e:
        print(f"ERROR: {e}")
        print("Please start the Flask API server first: uv run python src/api_server.py")
        sys.exit(1)

    # Load or create data
    data_file = "data/data.txt"
    if not Path(data_file).exists():
        create_sample_data(data_file)

    # Load data into DataFrame
    try:
        print(f"\nLoading data from {data_file}...")
        df = pd.read_csv(
            data_file,
            sep="|",
            header=None,
            names=["col1", "col2", "col3", "col4"],
        )
        df["line"] = df.apply(lambda row: "|".join(row.astype(str)), axis=1)
        print(f"Loaded {len(df)} lines")
        print("\nFirst few lines:")
        print(df.head())

        # Estimate processing time
        print(f"\n{'='*60}")
        print("TIME ESTIMATES (with random delays 10ms-2000ms per API call):")
        print(f"{'='*60}")
        avg_delay_per_call = (0.01 + 2.0) / 2  # Average of min and max
        avg_delay_per_line = avg_delay_per_call * 2  # Two API calls per line

        print(f"Sequential (1 thread): ~{len(df) * avg_delay_per_line / 60:.1f} minutes")
        print(f"Dask DataFrame: ~{len(df) * avg_delay_per_line / (CHUNK_SIZE * 0.8) / 60:.1f} minutes")
        print(f"Dask Delayed: ~{len(df) * avg_delay_per_line / (50 * 0.8) / 60:.1f} minutes")
        print(f"Expected speedup: ~{CHUNK_SIZE * 0.8:.1f}x (DataFrame) / ~{50 * 0.8:.1f}x (Delayed)")
        print(f"{'='*60}")

        # Ask user for sample size
        while True:
            choice = input("\nProcess all lines? (y/n, or enter number for sample size): ").strip()
            if choice.lower() == "y":
                break
            elif choice.lower() == "n":
                return
            else:
                try:
                    sample_size = int(choice)
                    if 0 < sample_size <= len(df):
                        df = df.head(sample_size)
                        print(f"\nProcessing sample of {len(df)} lines")
                        break
                    else:
                        print(f"Please enter a number between 1 and {len(df)}")
                except ValueError:
                    print("Please enter 'y', 'n', or a number")

        # Ask which Dask method to use
        print("\nChoose Dask processing method:")
        print("1. Dask DataFrame (recommended for large datasets)")
        print("2. Dask Delayed (more control over batching)")
        print("3. Both (for comparison)")

        while True:
            method_choice = input("Enter choice (1/2/3): ").strip()
            if method_choice in ["1", "2", "3"]:
                break
            print("Please enter 1, 2, or 3")

        # Process with selected method(s)
        if method_choice in ["1", "3"]:
            # Dask DataFrame method
            results_df_dask = process_dataframe_dask(df)

        if method_choice in ["2", "3"]:
            # Dask Delayed method
            results_df_delayed = process_with_dask_delayed(df)

        # Performance comparison
        if method_choice == "3" and len(df) <= 500:
            print(f"\n{'='*60}")
            print("DASK METHODS COMPARISON")
            print(f"{'='*60}")
            # We would need to capture timing from both methods for comparison
            print("(Both methods completed - check individual statistics above)")
            print(f"{'='*60}\n")

        # Save results and show samples
        if method_choice == "1":
            final_results = results_df_dask
        elif method_choice == "2":
            final_results = results_df_delayed
        else:
            final_results = results_df_dask  # Use DataFrame results as default

        final_results.to_csv("data/processed_data_dask.csv", index=False)
        print("Results saved to data/processed_data_dask.csv")

        # Show sample results
        print("\nSample results:")
        print(
            final_results[
                ["line", "capitalized", "length", "api_time_ms", "json_parse_time_ms"]
            ].head(10)
        )

        # Show distribution of times
        print("\nAPI Time Distribution:")
        print(final_results["api_time_ms"].describe())

        print("\nJSON Parse Time Distribution (orjson):")
        print(final_results["json_parse_time_ms"].describe())

    except Exception as e:
        print(f"Error processing data: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()