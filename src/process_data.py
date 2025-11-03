import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from pathlib import Path
from threading import Lock

import httpx
import orjson
import pandas as pd

# Configuration
API_BASE_URL = "http://localhost:5000"
MAX_WORKERS = 20  # Number of concurrent threads


# Thread-safe counter for progress tracking and API timing
class ProgressTracker:
    def __init__(self, total):
        self.total = total
        self.processed = 0
        self.lock = Lock()
        self.api_times = []  # Track API response times
        self.json_parse_times = []  # Track JSON parsing times

    def increment(self, api_time=None, parse_time=None):
        with self.lock:
            self.processed += 1
            if api_time:
                self.api_times.append(api_time)
            if parse_time:
                self.json_parse_times.append(parse_time)

            if self.processed % 1000 == 0:
                avg_time = statistics.mean(self.api_times) if self.api_times else 0
                avg_parse = statistics.mean(self.json_parse_times) if self.json_parse_times else 0
                print(
                    f"Progress: {self.processed}/{self.total} lines processed | "
                    f"Avg API time: {avg_time:.2f}ms | "
                    f"Avg JSON parse: {avg_parse:.4f}ms"
                )

    def get_stats(self):
        api_stats = {}
        parse_stats = {}

        if self.api_times:
            api_stats = {
                "min": min(self.api_times),
                "max": max(self.api_times),
                "mean": statistics.mean(self.api_times),
                "median": statistics.median(self.api_times),
                "stdev": statistics.stdev(self.api_times) if len(self.api_times) > 1 else 0,
            }

        if self.json_parse_times:
            parse_stats = {
                "min": min(self.json_parse_times),
                "max": max(self.json_parse_times),
                "mean": statistics.mean(self.json_parse_times),
                "median": statistics.median(self.json_parse_times),
                "stdev": (
                    statistics.stdev(self.json_parse_times)
                    if len(self.json_parse_times) > 1
                    else 0
                ),
                "total": sum(self.json_parse_times),
            }

        return {"api": api_stats, "parse": parse_stats}


class APIClient:
    """
    API Client class that manages HTTP connections and makes API calls
    Uses httpx.Client for connection pooling and better performance
    """

    def __init__(self, base_url: str, timeout: float = 10.0):
        """
        Initialize API client with persistent HTTP client

        Args:
            base_url: Base URL for the API
            timeout: Request timeout in seconds
        """
        self.base_url = base_url
        self.timeout = timeout
        # Create a persistent client with connection pooling
        self.client = httpx.Client(
            base_url=base_url,
            timeout=timeout,
            limits=httpx.Limits(
                max_connections=100,  # Maximum number of connections in the pool
                max_keepalive_connections=20,  # Keep connections alive for reuse
            ),
            http2=True,  # Enable HTTP/2 for better performance
        )

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close the client"""
        self.close()

    def close(self):
        """Close the HTTP client"""
        self.client.close()

    def call_capitalize_api(self, line: str) -> tuple[str | None, float, float]:
        """
        Make API call to capitalize endpoint

        Args:
            line: Input line to process

        Returns:
            Tuple of (capitalized_word, api_time_ms, parse_time_ms)
        """
        try:
            # Serialize request with orjson
            json_data = orjson.dumps({"line": line})

            response = self.client.post(
                "/capitalize",
                content=json_data,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()

            # Parse response with orjson and measure time
            parse_start = time.perf_counter()
            data = orjson.loads(response.content)
            parse_time = (time.perf_counter() - parse_start) * 1000  # Convert to ms

            return data.get("capitalized", ""), data.get("processing_time", 0), parse_time
        except Exception as e:
            print(f"Error in capitalize API: {e}")
            return None, 0, 0

    def call_length_api(self, line: str) -> tuple[int | None, float, float]:
        """
        Make API call to line length endpoint

        Args:
            line: Input line to process

        Returns:
            Tuple of (length, api_time_ms, parse_time_ms)
        """
        try:
            # Serialize request with orjson
            json_data = orjson.dumps({"line": line})

            response = self.client.post(
                "/line_length",
                content=json_data,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()

            # Parse response with orjson and measure time
            parse_start = time.perf_counter()
            data = orjson.loads(response.content)
            parse_time = (time.perf_counter() - parse_start) * 1000  # Convert to ms

            return data.get("length", 0), data.get("processing_time", 0), parse_time
        except Exception as e:
            print(f"Error in length API: {e}")
            return None, 0, 0

    def process_line(self, line: str, tracker: ProgressTracker) -> dict:
        """
        Process a single line by making both API calls

        Args:
            line: Input line to process
            tracker: Progress tracker instance

        Returns:
            Dictionary with processing results
        """
        # Make both API calls and track their times
        capitalized, time1, parse_time1 = self.call_capitalize_api(line)
        length, time2, parse_time2 = self.call_length_api(line)

        # Total API time and parse time for this line
        total_api_time = time1 + time2
        total_parse_time = parse_time1 + parse_time2

        # Update progress
        tracker.increment(total_api_time, total_parse_time)

        return {
            "capitalized": capitalized,
            "length": length,
            "api_time_ms": total_api_time,
            "json_parse_time_ms": total_parse_time,
        }

    def check_health(self) -> dict:
        """
        Check API health status

        Returns:
            Health status dictionary
        """
        try:
            response = self.client.get("/health")
            response.raise_for_status()
            return orjson.loads(response.content)
        except Exception as e:
            raise ConnectionError(f"Cannot connect to API: {e}") from e


def create_sample_data(filename="data/data.txt", lines=40000):
    """Create a sample pipe-separated file for testing"""
    print(f"Creating sample data file with {lines} lines...")

    import random
    import string

    # Ensure data directory exists
    Path(filename).parent.mkdir(parents=True, exist_ok=True)

    with open(filename, "w") as f:
        for _i in range(lines):
            # Generate random pipe-separated data
            word1 = "".join(random.choices(string.ascii_lowercase, k=random.randint(5, 15)))
            word2 = "".join(random.choices(string.ascii_lowercase, k=random.randint(5, 15)))
            word3 = "".join(random.choices(string.digits, k=random.randint(3, 10)))
            word4 = "".join(random.choices(string.ascii_lowercase, k=random.randint(5, 20)))

            line = f"{word1}|{word2}|{word3}|{word4}\n"
            f.write(line)

    print(f"Sample data file '{filename}' created successfully!")


def process_row(row, api_client: APIClient, tracker: ProgressTracker):
    """
    Process a single row by making both API calls
    This function will be applied to each row of the DataFrame

    Args:
        row: DataFrame row
        api_client: APIClient instance for making API calls
        tracker: ProgressTracker instance

    Returns:
        Series with results
    """
    line = row["line"]
    result = api_client.process_line(line, tracker)

    # Return a Series with results
    return pd.Series(result)


def parallel_apply(df, func, max_workers=MAX_WORKERS):
    """
    Apply function to DataFrame rows in parallel using ThreadPoolExecutor

    Args:
        df: pandas DataFrame
        func: function to apply to each row (should accept a row and return a Series)
        max_workers: number of threads

    Returns:
        DataFrame with results
    """
    # Create a list to store results in order
    results = [None] * len(df)

    # Use ThreadPoolExecutor to process rows in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks and map them to their indices
        future_to_index = {executor.submit(func, row): idx for idx, row in df.iterrows()}

        # Collect results as they complete
        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            try:
                results[idx] = future.result()
            except Exception as e:
                print(f"Error processing row {idx}: {e}")
                # Return empty Series on error
                results[idx] = pd.Series(
                    {
                        "capitalized": None,
                        "length": None,
                        "api_time_ms": 0,
                        "json_parse_time_ms": 0,
                    }
                )

    # Combine all results into a DataFrame
    results_df = pd.DataFrame(results)
    return results_df


def process_with_multithreading_apply(df, api_client: APIClient, max_workers=MAX_WORKERS):
    """Process dataframe using ThreadPool with df.apply pattern"""
    print(f"\n{'='*60}")
    print(f"Starting multithreaded processing with {max_workers} workers")
    print("Method: ThreadPoolExecutor with DataFrame.apply pattern")
    print("HTTP Client: httpx with connection pooling")
    print("JSON Parser: orjson (high-performance)")
    print("Project Manager: uv (ultra-fast)")
    print("API Latency: 10ms - 2000ms per call (random)")
    print("Expected range: 20ms - 4000ms per line (2 API calls)")
    print(f"{'='*60}\n")

    start_time = time.time()

    # Create progress tracker
    tracker = ProgressTracker(len(df))

    # Create partial function with api_client and tracker bound
    process_func = partial(process_row, api_client=api_client, tracker=tracker)

    # Apply function in parallel using ThreadPoolExecutor
    results_df = parallel_apply(df, process_func, max_workers=max_workers)

    # Combine original data with results
    final_df = pd.concat([df.reset_index(drop=True), results_df], axis=1)

    end_time = time.time()
    elapsed_time = end_time - start_time

    # Get statistics
    stats = tracker.get_stats()
    api_stats = stats.get("api", {})
    parse_stats = stats.get("parse", {})

    # Print statistics
    print(f"\n{'='*60}")
    print("Processing Complete!")
    print(f"{'='*60}")
    print(f"Total lines processed: {len(final_df)}")
    print(f"Total time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    print(f"Average time per line: {elapsed_time/len(final_df)*1000:.2f} ms")
    print(f"Throughput: {len(final_df)/elapsed_time:.2f} lines/second")
    print(f"Total API calls made: {len(final_df) * 2}")

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

    return final_df, elapsed_time


def process_sequentially_apply(df, api_client: APIClient):
    """Process dataframe sequentially using apply (for comparison)"""
    print(f"\n{'='*60}")
    print("Starting sequential processing with df.apply")
    print("HTTP Client: httpx with connection pooling")
    print("JSON Parser: orjson (high-performance)")
    print("WARNING: This will take a LONG time with random delays!")
    print(f"{'='*60}\n")

    start_time = time.time()

    # Create progress tracker
    tracker = ProgressTracker(len(df))

    # Create partial function with api_client and tracker
    process_func = partial(process_row, api_client=api_client, tracker=tracker)

    # Apply function sequentially
    results_df = df.apply(process_func, axis=1)

    # Combine original data with results
    final_df = pd.concat([df.reset_index(drop=True), results_df], axis=1)

    end_time = time.time()
    elapsed_time = end_time - start_time

    # Get statistics
    stats = tracker.get_stats()
    api_stats = stats.get("api", {})
    parse_stats = stats.get("parse", {})

    # Print statistics
    print(f"\n{'='*60}")
    print("Processing Complete!")
    print(f"{'='*60}")
    print(f"Total lines processed: {len(final_df)}")
    print(f"Total time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    print(f"Average time per line: {elapsed_time/len(final_df)*1000:.2f} ms")
    print(f"Throughput: {len(final_df)/elapsed_time:.2f} lines/second")

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

    return final_df, elapsed_time


def main():
    # File configuration
    input_file = "data/data.txt"
    output_file = "data/processed_data.csv"

    # Check if sample data exists, if not create it
    if not Path(input_file).exists():
        create_sample_data(input_file, lines=40000)

    # Create API client with context manager for automatic cleanup
    with APIClient(API_BASE_URL) as api_client:
        # Check if API is running
        try:
            health_data = api_client.check_health()
            print(f"API Status: {health_data}")
        except ConnectionError as e:
            print(f"ERROR: {e}")
            print("Please start the Flask API server first: uv run python src/api_server.py")
            sys.exit(1)

        # Load data using pandas
        print(f"\nLoading data from {input_file}...")
        df = pd.read_csv(input_file, sep="|", header=None, names=["col1", "col2", "col3", "col4"])

        # Combine all columns back into a single line for processing
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

        sequential_estimate = len(df) * avg_delay_per_line / 60
        multithreaded_estimate = len(df) * avg_delay_per_line / MAX_WORKERS / 60

        print(f"Sequential (1 thread): ~{sequential_estimate:.1f} minutes")
        print(f"Multithreaded ({MAX_WORKERS} threads): ~{multithreaded_estimate:.1f} minutes")
        print(f"Expected speedup: ~{sequential_estimate/multithreaded_estimate:.1f}x")
        print(f"{'='*60}\n")

        # Option to test with smaller dataset
        test_sample = (
            input(f"Process all {len(df)} lines? (y/n, or enter number for sample size): ")
            .strip()
            .lower()
        )
        if test_sample == "n":
            sample_size = int(input("Enter sample size: "))
            df = df.head(sample_size)
            print(f"\nProcessing sample of {len(df)} lines")
        elif test_sample.isdigit():
            sample_size = int(test_sample)
            df = df.head(sample_size)
            print(f"\nProcessing sample of {len(df)} lines")

        # Process with multithreading using apply pattern
        results_df_mt, time_mt = process_with_multithreading_apply(
            df, api_client, max_workers=MAX_WORKERS
        )

        # Optional: Compare with sequential processing (only for small samples)
        if len(df) <= 500:
            compare = (
                input("\nWould you like to compare with sequential df.apply? (y/n): ")
                .strip()
                .lower()
            )
            if compare == "y":
                results_df_seq, time_seq = process_sequentially_apply(df, api_client)

                print(f"\n{'='*60}")
                print("PERFORMANCE COMPARISON")
                print(f"{'='*60}")
                print(
                    f"Sequential (df.apply): {time_seq:.2f} seconds ({time_seq/60:.2f} minutes)"
                )
                print(
                    f"Multithreaded (ThreadPool + apply): {time_mt:.2f} seconds ({time_mt/60:.2f} minutes)"
                )
                print(f"Speedup: {time_seq/time_mt:.2f}x faster with multithreading")
                print(f"Time saved: {(time_seq-time_mt)/60:.2f} minutes")
                print(f"{'='*60}\n")
        else:
            print("\n(Sequential comparison skipped - sample size > 500 lines)")
            print(
                f"(Sequential processing would take ~{len(df) * avg_delay_per_line / 60:.1f} minutes)"
            )

        # Save results
        results_df_mt.to_csv(output_file, index=False)
        print(f"Results saved to {output_file}")

        # Show sample results
        print("\nSample results:")
        print(
            results_df_mt[
                ["line", "capitalized", "length", "api_time_ms", "json_parse_time_ms"]
            ].head(10)
        )

        # Show distribution of times
        print("\nAPI Time Distribution:")
        print(results_df_mt["api_time_ms"].describe())

        print("\nJSON Parse Time Distribution (orjson):")
        print(results_df_mt["json_parse_time_ms"].describe())


if __name__ == "__main__":
    main()
