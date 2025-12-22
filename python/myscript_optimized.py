#!/usr/bin/env python3
"""
Optimized Log Aggregation Script - Handle millions of records efficiently.

Features:
- Multi-threaded file reading
- Memory-efficient streaming
- Chunked processing
- Detailed CPU/Memory metrics (cores, %, frequency)

Usage:
    python myscript_optimized.py --from_datetime="2025-01-01 00:00:00" --to_datetime="2025-03-31 23:59:59" \
                                  --granularity=1day --dimensions=user [--threads=4]
"""

import argparse
import csv
import os
import time
import resource
import threading
from datetime import datetime, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import mmap

# Configuration
LOGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'csv_logs')
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'results')
METRIC_COLUMNS = [f'metric_{i}' for i in range(1, 10)]

# Thread-safe aggregation lock
aggregation_lock = threading.Lock()

def get_cpu_info():
    """Get detailed CPU information (Linux)."""
    cpu_info = {
        'cores': os.cpu_count() or 1,
        'freq_mhz': None,
        'model': None
    }
    
    try:
        with open('/proc/cpuinfo', 'r') as f:
            for line in f:
                if line.startswith('model name'):
                    cpu_info['model'] = line.split(':')[1].strip()
                elif line.startswith('cpu MHz'):
                    cpu_info['freq_mhz'] = float(line.split(':')[1].strip())
                    break
    except (FileNotFoundError, PermissionError):
        pass
    
    return cpu_info

def get_cpu_times():
    """Get CPU times from /proc/stat (Linux)."""
    try:
        with open('/proc/stat', 'r') as f:
            line = f.readline()
            parts = line.split()
            # user, nice, system, idle, iowait, irq, softirq
            return {
                'user': int(parts[1]),
                'nice': int(parts[2]),
                'system': int(parts[3]),
                'idle': int(parts[4]),
                'iowait': int(parts[5]) if len(parts) > 5 else 0,
            }
    except (FileNotFoundError, PermissionError):
        return None

def calc_cpu_percent(before, after):
    """Calculate CPU usage percentage."""
    if not before or not after:
        return None
    
    total_before = sum(before.values())
    total_after = sum(after.values())
    idle_before = before['idle'] + before.get('iowait', 0)
    idle_after = after['idle'] + after.get('iowait', 0)
    
    total_diff = total_after - total_before
    idle_diff = idle_after - idle_before
    
    if total_diff == 0:
        return 0.0
    
    return ((total_diff - idle_diff) / total_diff) * 100

def get_memory_info():
    """Get memory information."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    mem_info = {
        'rss_mb': usage.ru_maxrss / 1024,  # Convert KB to MB
    }
    
    # Try to get total system memory
    try:
        with open('/proc/meminfo', 'r') as f:
            for line in f:
                if line.startswith('MemTotal'):
                    mem_info['total_mb'] = int(line.split()[1]) / 1024
                    break
    except (FileNotFoundError, PermissionError):
        pass
    
    return mem_info

def get_io_stats():
    """Get I/O statistics."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        'read_ops': usage.ru_inblock,
        'write_ops': usage.ru_oublock
    }

def parse_datetime(dt_str):
    """Parse datetime string to datetime object."""
    return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')

def get_period_start(timestamp, granularity):
    """Get the start of the period (30m or 1day) for a given timestamp."""
    if granularity == '30m':
        minute = (timestamp.minute // 30) * 30
        return timestamp.replace(minute=minute, second=0, microsecond=0)
    elif granularity == '1day':
        return timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        raise ValueError(f"Invalid granularity: {granularity}")

def get_date_range(from_dt, to_dt):
    """Get list of dates between from_dt and to_dt (inclusive)."""
    dates = []
    current = from_dt.date()
    end = to_dt.date()
    
    while current <= end:
        dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
    
    return dates

def process_file_chunk(filepath, from_dt, to_dt, granularity, dimensions, 
                       user_filter, app_filter, chunk_start=0, chunk_size=None):
    """
    Process a chunk of a file and return partial aggregation.
    Memory-efficient streaming approach.
    """
    local_aggregated = defaultdict(lambda: [0] * 9)
    rows_processed = 0
    
    user_set = set(user_filter) if user_filter else None
    app_set = set(app_filter) if app_filter else None
    
    if not os.path.exists(filepath):
        return local_aggregated, 0
    
    with open(filepath, 'r', newline='') as f:
        # Skip to chunk start if specified
        if chunk_start > 0:
            for _ in range(chunk_start):
                next(f, None)
        
        reader = csv.reader(f)
        rows_read = 0
        
        for row in reader:
            if chunk_size and rows_read >= chunk_size:
                break
            
            rows_read += 1
            
            if len(row) < 12:
                continue
            
            timestamp_str, user, app = row[0], row[1], row[2]
            
            # Filter by user if specified
            if user_set and user not in user_set:
                continue
            
            # Filter by app if specified
            if app_set and app not in app_set:
                continue
            
            # Parse and filter by datetime
            try:
                row_dt = parse_datetime(timestamp_str)
            except ValueError:
                continue
            
            if row_dt < from_dt or row_dt >= to_dt:
                continue
            
            rows_processed += 1
            
            # Calculate period start
            period_start = get_period_start(row_dt, granularity)
            period_str = period_start.strftime('%Y-%m-%d %H:%M:%S')
            
            # Build dimension key
            dim_values = []
            if 'user' in dimensions:
                dim_values.append(user)
            if 'app' in dimensions:
                dim_values.append(app)
            
            key = (period_str, *dim_values)
            
            # Aggregate metrics
            metrics = [int(row[i]) for i in range(3, 12)]
            for i in range(9):
                local_aggregated[key][i] += metrics[i]
    
    return local_aggregated, rows_processed

def process_file_parallel(filepath, from_dt, to_dt, granularity, dimensions,
                          user_filter, app_filter, num_threads=4):
    """Process a single large file using multiple threads."""
    
    # Count lines in file
    line_count = 0
    with open(filepath, 'r') as f:
        for _ in f:
            line_count += 1
    
    if line_count == 0:
        return defaultdict(lambda: [0] * 9), 0
    
    # For small files, process directly
    if line_count < 100000 or num_threads == 1:
        return process_file_chunk(filepath, from_dt, to_dt, granularity, 
                                  dimensions, user_filter, app_filter)
    
    # Split into chunks for parallel processing
    chunk_size = (line_count + num_threads - 1) // num_threads
    
    combined = defaultdict(lambda: [0] * 9)
    total_rows = 0
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in range(num_threads):
            chunk_start = i * chunk_size
            futures.append(executor.submit(
                process_file_chunk,
                filepath, from_dt, to_dt, granularity, dimensions,
                user_filter, app_filter, chunk_start, chunk_size
            ))
        
        for future in as_completed(futures):
            partial, rows = future.result()
            total_rows += rows
            
            # Merge partial results
            for key, metrics in partial.items():
                for i in range(9):
                    combined[key][i] += metrics[i]
    
    return combined, total_rows

def aggregate_data_parallel(from_dt, to_dt, granularity, dimensions, 
                            user_filter=None, app_filter=None, num_threads=4):
    """Aggregate log data using parallel processing."""
    date_files = get_date_range(from_dt, to_dt)
    
    combined = defaultdict(lambda: [0] * 9)
    total_rows = 0
    files_processed = 0
    
    # Process files in parallel
    with ThreadPoolExecutor(max_workers=min(num_threads, len(date_files))) as executor:
        futures = {}
        
        for date_str in date_files:
            filepath = os.path.join(LOGS_DIR, f'{date_str}.log.csv')
            if os.path.exists(filepath):
                future = executor.submit(
                    process_file_chunk,
                    filepath, from_dt, to_dt, granularity, dimensions,
                    user_filter, app_filter
                )
                futures[future] = date_str
        
        for future in as_completed(futures):
            partial, rows = future.result()
            total_rows += rows
            files_processed += 1
            
            # Merge results
            for key, metrics in partial.items():
                for i in range(9):
                    combined[key][i] += metrics[i]
    
    return combined, total_rows, files_processed

def get_next_output_filename(base_name):
    """Get next available output filename with auto-increment."""
    import re
    os.makedirs(RESULTS_DIR, exist_ok=True)
    
    name, ext = os.path.splitext(base_name)
    if not ext:
        ext = '.csv'
    
    base_path = os.path.join(RESULTS_DIR, base_name)
    if not os.path.exists(base_path):
        return base_name
    
    pattern = re.compile(rf'^{re.escape(name)}_(\d+){re.escape(ext)}$')
    max_num = 0
    
    for filename in os.listdir(RESULTS_DIR):
        match = pattern.match(filename)
        if match:
            max_num = max(max_num, int(match.group(1)))
    
    return f"{name}_{max_num + 1}{ext}"

def write_results(aggregated, dimensions, output_file):
    """Write aggregated results to CSV file."""
    os.makedirs(RESULTS_DIR, exist_ok=True)
    
    actual_output = get_next_output_filename(output_file)
    
    header = ['timestamp']
    if 'user' in dimensions:
        header.append('user')
    if 'app' in dimensions:
        header.append('app')
    header.extend(METRIC_COLUMNS)
    
    sorted_keys = sorted(aggregated.keys())
    
    filepath = os.path.join(RESULTS_DIR, actual_output)
    with open(filepath, 'w', newline='', buffering=65536) as f:
        writer = csv.writer(f)
        writer.writerow(header)
        
        for key in sorted_keys:
            row = list(key) + aggregated[key]
            writer.writerow(row)
    
    print(f"Results saved to: {filepath}")
    return filepath

def format_time(seconds):
    """Format seconds to human-readable time."""
    if seconds < 1:
        return f"{seconds*1000:.2f}ms"
    elif seconds < 60:
        return f"{seconds:.3f}s"
    else:
        mins = int(seconds // 60)
        secs = seconds % 60
        return f"{mins}m {secs:.1f}s"

def print_system_info():
    """Print system information."""
    cpu_info = get_cpu_info()
    mem_info = get_memory_info()
    
    print("=" * 70)
    print("SYSTEM INFORMATION")
    print("=" * 70)
    print(f"  CPU Model:      {cpu_info.get('model', 'Unknown')}")
    print(f"  CPU Cores:      {cpu_info['cores']}")
    if cpu_info.get('freq_mhz'):
        print(f"  CPU Frequency:  {cpu_info['freq_mhz']:.0f} MHz ({cpu_info['freq_mhz']/1000:.2f} GHz)")
    if mem_info.get('total_mb'):
        print(f"  Total Memory:   {mem_info['total_mb']/1024:.1f} GB")
    print("=" * 70)

def main():
    parser = argparse.ArgumentParser(
        description='Optimized log aggregation with multi-threading support.',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--from_datetime', '--from-datetime',
                        dest='from_datetime', required=True,
                        help='Start datetime (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--to_datetime', '--to-datetime',
                        dest='to_datetime', required=True,
                        help='End datetime (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--granularity', required=True, choices=['30m', '1day'],
                        help='Aggregation granularity')
    parser.add_argument('--dimensions', required=True,
                        help='Dimensions to group by (user, app, or user,app)')
    parser.add_argument('--user', help='Filter by user(s), comma-separated')
    parser.add_argument('--app', help='Filter by app(s), comma-separated')
    parser.add_argument('--output', default='result.csv',
                        help='Output filename (default: result.csv)')
    parser.add_argument('--threads', type=int, default=4,
                        help='Number of worker threads (default: 4)')
    
    args = parser.parse_args()
    
    # Print system info
    print_system_info()
    print()
    
    # Parse arguments
    from_dt = parse_datetime(args.from_datetime)
    to_dt = parse_datetime(args.to_datetime)
    
    dimensions = [d.strip() for d in args.dimensions.split(',')]
    valid_dims = {'user', 'app'}
    for dim in dimensions:
        if dim not in valid_dims:
            parser.error(f"Invalid dimension: {dim}")
    
    user_filter = [u.strip() for u in args.user.split(',')] if args.user else None
    app_filter = [a.strip() for a in args.app.split(',')] if args.app else None
    
    print(f"Query: {from_dt} to {to_dt}")
    print(f"Granularity: {args.granularity} | Dimensions: {dimensions}")
    print(f"Threads: {args.threads}")
    if user_filter:
        print(f"User filter: {user_filter}")
    if app_filter:
        print(f"App filter: {app_filter}")
    print()
    
    # Start metrics collection
    start_time = time.perf_counter()
    cpu_before = get_cpu_times()
    io_before = get_io_stats()
    
    # Aggregate data
    print("Processing files...")
    aggregated, total_rows, files_processed = aggregate_data_parallel(
        from_dt, to_dt, args.granularity, dimensions, 
        user_filter, app_filter, args.threads
    )
    
    read_time = time.perf_counter() - start_time
    cpu_after_read = get_cpu_times()
    io_after_read = get_io_stats()
    
    print(f"Processed {files_processed} files, {total_rows:,} rows matched")
    print(f"Found {len(aggregated)} aggregated entries")
    
    # Write results
    write_time = 0
    if aggregated:
        write_start = time.perf_counter()
        write_results(aggregated, dimensions, args.output)
        write_time = time.perf_counter() - write_start
    else:
        print("No data found matching criteria.")
    
    total_time = time.perf_counter() - start_time
    cpu_after = get_cpu_times()
    io_after = get_io_stats()
    
    # Calculate metrics
    cpu_percent_read = calc_cpu_percent(cpu_before, cpu_after_read)
    cpu_percent_total = calc_cpu_percent(cpu_before, cpu_after)
    mem_info = get_memory_info()
    cpu_info = get_cpu_info()
    
    # Print performance summary
    print()
    print("=" * 70)
    print("PERFORMANCE METRICS")
    print("=" * 70)
    print(f"{'Phase':<30} {'Time':>12} {'Rows/sec':>15}")
    print("-" * 70)
    rows_per_sec_read = total_rows / read_time if read_time > 0 else 0
    print(f"{'Data reading & aggregation':<30} {format_time(read_time):>12} {rows_per_sec_read:>12,.0f}")
    if write_time > 0:
        print(f"{'Writing results':<30} {format_time(write_time):>12}")
    print("-" * 70)
    print(f"{'TOTAL':<30} {format_time(total_time):>12}")
    print("=" * 70)
    print()
    print("CPU METRICS")
    print("-" * 70)
    print(f"  CPU Cores Used:      {args.threads} / {cpu_info['cores']} available")
    if cpu_info.get('freq_mhz'):
        print(f"  CPU Frequency:       {cpu_info['freq_mhz']:.0f} MHz")
    if cpu_percent_total is not None:
        print(f"  CPU Usage (read):    {cpu_percent_read:.1f}%")
        print(f"  CPU Usage (total):   {cpu_percent_total:.1f}%")
    print()
    print("MEMORY & I/O")
    print("-" * 70)
    print(f"  Peak Memory:         {mem_info['rss_mb']:.2f} MB")
    print(f"  I/O Read Ops:        {io_after['read_ops'] - io_before['read_ops']}")
    print(f"  I/O Write Ops:       {io_after['write_ops'] - io_before['write_ops']}")
    print("=" * 70)

if __name__ == '__main__':
    main()
