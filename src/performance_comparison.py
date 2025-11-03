"""
Comprehensive Performance Comparison Script
Compares Sequential, Multi-threaded, and Dask processing approaches
"""
import pandas as pd
import time
import sys
from pathlib import Path

# Import from our existing modules
sys.path.append(str(Path(__file__).parent))

from process_data import (
    APIClient, 
    process_with_multithreading_apply,
    process_sequentially_apply
)
from process_data_dask import (
    DaskAPIClient,
    process_dataframe_dask,
    process_with_dask_delayed
)

# Configuration
API_BASE_URL = "http://localhost:5000"
SAMPLE_SIZES = [50, 100, 200]  # Different sample sizes to test


def test_api_connection():
    """Test if API server is running"""
    try:
        with APIClient(API_BASE_URL) as api_client:
            health_data = api_client.check_health()
            print(f"✅ API Status: {health_data}")
            return True
    except Exception as e:
        print(f"❌ ERROR: Cannot connect to API server at {API_BASE_URL}")
        print(f"Error: {e}")
        print("Please start the Flask API server first: uv run python src/api_server.py")
        return False


def load_test_data(sample_size: int = None):
    """Load test data"""
    data_file = "data/data.txt"
    if not Path(data_file).exists():
        print(f"❌ Data file {data_file} not found!")
        return None
    
    try:
        print(f"📁 Loading data from {data_file}...")
        df = pd.read_csv(
            data_file,
            sep="|",
            header=None,
            names=["col1", "col2", "col3", "col4"],
        )
        df["line"] = df.apply(lambda row: "|".join(row.astype(str)), axis=1)
        
        if sample_size and sample_size < len(df):
            df = df.head(sample_size)
            
        print(f"✅ Loaded {len(df)} lines")
        return df
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return None


def run_sequential_test(df: pd.DataFrame):
    """Run sequential processing test"""
    print(f"\n{'='*60}")
    print("🐌 SEQUENTIAL PROCESSING TEST")
    print(f"{'='*60}")
    
    start_time = time.time()
    try:
        with APIClient(API_BASE_URL) as api_client:
            results_df = process_sequentially_apply(df, api_client)
            elapsed_time = time.time() - start_time
        
        return {
            'method': 'Sequential',
            'success': True,
            'time': elapsed_time,
            'throughput': len(df) / elapsed_time,
            'lines_processed': len(results_df),
            'error': None
        }
    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"❌ Sequential processing failed: {e}")
        return {
            'method': 'Sequential',
            'success': False,
            'time': elapsed_time,
            'throughput': 0,
            'lines_processed': 0,
            'error': str(e)
        }


def run_multithreaded_test(df: pd.DataFrame):
    """Run multi-threaded processing test"""
    print(f"\n{'='*60}")
    print("🚀 MULTI-THREADED PROCESSING TEST")
    print(f"{'='*60}")
    
    start_time = time.time()
    try:
        with APIClient(API_BASE_URL) as api_client:
            results_df = process_with_multithreading_apply(df, api_client)
            elapsed_time = time.time() - start_time
        
        return {
            'method': 'Multi-threaded',
            'success': True,
            'time': elapsed_time,
            'throughput': len(df) / elapsed_time,
            'lines_processed': len(results_df),
            'error': None
        }
    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"❌ Multi-threaded processing failed: {e}")
        return {
            'method': 'Multi-threaded',
            'success': False,
            'time': elapsed_time,
            'throughput': 0,
            'lines_processed': 0,
            'error': str(e)
        }


def run_dask_dataframe_test(df: pd.DataFrame):
    """Run Dask DataFrame processing test"""
    print(f"\n{'='*60}")
    print("⚡ DASK DATAFRAME PROCESSING TEST")
    print(f"{'='*60}")
    
    start_time = time.time()
    try:
        results_df = process_dataframe_dask(df)
        elapsed_time = time.time() - start_time
        
        return {
            'method': 'Dask DataFrame',
            'success': True,
            'time': elapsed_time,
            'throughput': len(df) / elapsed_time,
            'lines_processed': len(results_df),
            'error': None
        }
    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"❌ Dask DataFrame processing failed: {e}")
        return {
            'method': 'Dask DataFrame',
            'success': False,
            'time': elapsed_time,
            'throughput': 0,
            'lines_processed': 0,
            'error': str(e)
        }


def run_dask_delayed_test(df: pd.DataFrame):
    """Run Dask Delayed processing test"""
    print(f"\n{'='*60}")
    print("⚡ DASK DELAYED PROCESSING TEST")
    print(f"{'='*60}")
    
    start_time = time.time()
    try:
        results_df = process_with_dask_delayed(df, batch_size=25)
        elapsed_time = time.time() - start_time
        
        return {
            'method': 'Dask Delayed',
            'success': True,
            'time': elapsed_time,
            'throughput': len(df) / elapsed_time,
            'lines_processed': len(results_df),
            'error': None
        }
    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"❌ Dask Delayed processing failed: {e}")
        return {
            'method': 'Dask Delayed',
            'success': False,
            'time': elapsed_time,
            'throughput': 0,
            'lines_processed': 0,
            'error': str(e)
        }


def print_comparison_results(results: list, sample_size: int):
    """Print comparison results in a nice format"""
    print(f"\n{'='*80}")
    print(f"📊 PERFORMANCE COMPARISON RESULTS ({sample_size} lines)")
    print(f"{'='*80}")
    
    # Create a comparison table
    successful_results = [r for r in results if r['success']]
    
    if not successful_results:
        print("❌ No successful results to compare!")
        return
    
    # Sort by time (fastest first)
    successful_results.sort(key=lambda x: x['time'])
    
    print(f"{'Method':<20} {'Time (s)':<12} {'Throughput':<15} {'Speedup':<10} {'Status'}")
    print(f"{'-'*20} {'-'*12} {'-'*15} {'-'*10} {'-'*10}")
    
    baseline_time = successful_results[-1]['time']  # Slowest time as baseline
    
    for result in successful_results:
        speedup = baseline_time / result['time']
        status = "✅ Success" if result['success'] else "❌ Failed"
        
        print(f"{result['method']:<20} {result['time']:<12.2f} "
              f"{result['throughput']:<15.2f} {speedup:<10.2f}x {status}")
    
    # Show failed results
    failed_results = [r for r in results if not r['success']]
    if failed_results:
        print(f"\n❌ Failed Methods:")
        for result in failed_results:
            print(f"  {result['method']}: {result['error']}")
    
    # Best performer summary
    if successful_results:
        best = successful_results[0]
        worst = successful_results[-1]
        improvement = worst['time'] / best['time']
        
        print(f"\n🏆 WINNER: {best['method']}")
        print(f"   ⏱️  Time: {best['time']:.2f} seconds")
        print(f"   🚀 Throughput: {best['throughput']:.2f} lines/second")
        print(f"   📈 Improvement: {improvement:.2f}x faster than slowest method")


def main():
    """Main comparison function"""
    print("🔥 API Performance Testing Comparison Tool")
    print("=" * 60)
    
    # Test API connection
    if not test_api_connection():
        sys.exit(1)
    
    # Ask user for sample size
    print(f"\nAvailable sample sizes: {SAMPLE_SIZES}")
    while True:
        try:
            choice = input("Enter sample size (or custom number): ").strip()
            if choice.isdigit():
                sample_size = int(choice)
                if sample_size > 0:
                    break
            elif choice in [str(s) for s in SAMPLE_SIZES]:
                sample_size = int(choice)
                break
            print("Please enter a valid positive number")
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            sys.exit(0)
        except:
            print("Please enter a valid number")
    
    # Load test data
    df = load_test_data(sample_size)
    if df is None:
        sys.exit(1)
    
    print(f"\n🧪 Testing with {len(df)} lines")
    
    # Ask which methods to test
    print("\nWhich methods would you like to test?")
    print("1. All methods (recommended)")
    print("2. Only fast methods (Multi-threaded + Dask)")
    print("3. Custom selection")
    
    while True:
        try:
            method_choice = input("Enter choice (1/2/3): ").strip()
            if method_choice in ["1", "2", "3"]:
                break
            print("Please enter 1, 2, or 3")
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            sys.exit(0)
    
    # Determine which tests to run
    run_sequential = False
    run_multithreaded = True
    run_dask_df = True
    run_dask_delayed = True
    
    if method_choice == "1":
        run_sequential = sample_size <= 100  # Only run sequential for small samples
        if sample_size > 100:
            print(f"⚠️  Skipping sequential test for {sample_size} lines (would take too long)")
    elif method_choice == "2":
        pass  # Default settings are good
    elif method_choice == "3":
        run_sequential = input("Run sequential test? (y/n): ").lower().startswith('y')
        run_multithreaded = input("Run multi-threaded test? (y/n): ").lower().startswith('y')
        run_dask_df = input("Run Dask DataFrame test? (y/n): ").lower().startswith('y')
        run_dask_delayed = input("Run Dask Delayed test? (y/n): ").lower().startswith('y')
    
    # Run the tests
    results = []
    
    if run_sequential:
        results.append(run_sequential_test(df))
    
    if run_multithreaded:
        results.append(run_multithreaded_test(df))
    
    if run_dask_df:
        results.append(run_dask_dataframe_test(df))
    
    if run_dask_delayed:
        results.append(run_dask_delayed_test(df))
    
    # Print comparison results
    if results:
        print_comparison_results(results, sample_size)
    else:
        print("❌ No tests were run!")
    
    print(f"\n✨ Comparison complete!")


if __name__ == "__main__":
    main()