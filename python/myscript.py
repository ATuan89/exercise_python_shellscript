
import argparse
import csv
import os
import time
import resource
import re
from datetime import datetime, timedelta
from collections import defaultdict

# Configuration
LOGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'csv_logs')
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'results')
METRIC_COLUMNS = [f'metric_{i}' for i in range(1, 10)]

def parse_datetime(dt_str):
    return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')

def get_period_start(timestamp, granularity):
    if granularity == '30m':
        # Round down to nearest 30-minute interval
        minute = (timestamp.minute // 30) * 30
        return timestamp.replace(minute=minute, second=0, microsecond=0)
    elif granularity == '1day':
        return timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        raise ValueError(f"Invalid granularity: {granularity}")

def get_date_range(from_dt, to_dt):
    dates = []
    current = from_dt.date()
    end = to_dt.date()
    
    while current <= end:
        dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
    
    return dates

def parse_row(row):
    return {
        'timestamp': row[0],
        'user': row[1],
        'app': row[2],
        'metrics': [int(row[i]) for i in range(3, 12)]
    }

def read_log_file(filepath, from_dt, to_dt, user_filter=None, app_filter=None):
    """
    Read a log file and yield filtered rows.
    
    Since files are sorted by user, we can optimize by:
    - Early exit when we've passed the user range (if filtering by single user)
    - Skipping rows outside the datetime range
    """
    if not os.path.exists(filepath):
        return
    
    user_set = set(user_filter) if user_filter else None
    app_set = set(app_filter) if app_filter else None
    
    with open(filepath, 'r', newline='') as f:
        reader = csv.reader(f)
        
        for row in reader:
            if len(row) < 12:
                continue
            
            parsed = parse_row(row)
            
            # Filter by user if specified
            if user_set and parsed['user'] not in user_set:
                continue
            
            # Filter by app if specified
            if app_set and parsed['app'] not in app_set:
                continue
            
            # Filter by datetime range
            try:
                row_dt = parse_datetime(parsed['timestamp'])
            except ValueError:
                continue
            
            if row_dt < from_dt or row_dt >= to_dt:
                continue
            
            yield parsed, row_dt

def aggregate_data(from_dt, to_dt, granularity, dimensions, user_filter=None, app_filter=None):
    """
    Aggregate log data based on parameters.
    
    Returns a dict keyed by (period_start, *dimension_values) with summed metrics.
    """
    # Determine which date files to read
    date_files = get_date_range(from_dt, to_dt)
    
    # Aggregation dict: key -> list of 9 metric sums
    aggregated = defaultdict(lambda: [0] * 9)
    
    for date_str in date_files:
        filepath = os.path.join(LOGS_DIR, f'{date_str}.log.csv')
        
        for parsed, row_dt in read_log_file(filepath, from_dt, to_dt, user_filter, app_filter):
            # Calculate period start
            period_start = get_period_start(row_dt, granularity)
            period_str = period_start.strftime('%Y-%m-%d %H:%M:%S')
            
            # Build dimension key
            dim_values = []
            if 'user' in dimensions:
                dim_values.append(parsed['user'])
            if 'app' in dimensions:
                dim_values.append(parsed['app'])
            
            key = (period_str, *dim_values)
            
            # Aggregate metrics
            for i in range(9):
                aggregated[key][i] += parsed['metrics'][i]
    
    return aggregated

def get_resource_usage():
    """Get current resource usage (CPU time, memory, I/O)."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        'user_time': usage.ru_utime,      # User CPU time
        'sys_time': usage.ru_stime,        # System CPU time
        'max_rss': usage.ru_maxrss,        # Max resident set size (KB on Linux)
        'io_read': usage.ru_inblock,       # Block input operations
        'io_write': usage.ru_oublock       # Block output operations
    }

def calc_resource_diff(before, after):
    """Calculate the difference in resource usage."""
    return {
        'cpu_user': after['user_time'] - before['user_time'],
        'cpu_sys': after['sys_time'] - before['sys_time'],
        'memory_kb': after['max_rss'],  # Peak memory is cumulative
        'io_read': after['io_read'] - before['io_read'],
        'io_write': after['io_write'] - before['io_write']
    }

def get_next_output_filename(base_name):
    """
    Get the next available output filename.
    If result.csv exists, return result_1.csv, result_2.csv, etc.
    """
    os.makedirs(RESULTS_DIR, exist_ok=True)
    
    # Split base name into name and extension
    name, ext = os.path.splitext(base_name)
    if not ext:
        ext = '.csv'
    
    # Check if base file exists
    base_path = os.path.join(RESULTS_DIR, base_name)
    if not os.path.exists(base_path):
        return base_name
    
    # Find the highest existing number
    pattern = re.compile(rf'^{re.escape(name)}_(\d+){re.escape(ext)}$')
    max_num = 0
    
    for filename in os.listdir(RESULTS_DIR):
        match = pattern.match(filename)
        if match:
            num = int(match.group(1))
            max_num = max(max_num, num)
    
    return f"{name}_{max_num + 1}{ext}"

def write_results(aggregated, dimensions, output_file):
    """Write aggregated results to CSV file."""
    os.makedirs(RESULTS_DIR, exist_ok=True)
    
    # Get next available filename
    actual_output = get_next_output_filename(output_file)
    
    # Build header
    header = ['timestamp']
    if 'user' in dimensions:
        header.append('user')
    if 'app' in dimensions:
        header.append('app')
    header.extend(METRIC_COLUMNS)
    
    # Sort by timestamp, then by dimension values
    sorted_keys = sorted(aggregated.keys())
    
    filepath = os.path.join(RESULTS_DIR, actual_output)
    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        
        for key in sorted_keys:
            row = list(key) + aggregated[key]
            writer.writerow(row)
    
    print(f"Results saved to: {filepath}")
    return filepath

def main():
    # Start performance timer
    start_time = time.perf_counter()
    
    parser = argparse.ArgumentParser(
        description='Query and aggregate CSV log files.',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Support both --from_datetime and --from-datetime style
    parser.add_argument(
        '--from_datetime', '--from-datetime',
        dest='from_datetime',
        required=True,
        help='Start datetime (YYYY-MM-DD HH:MM:SS)'
    )
    parser.add_argument(
        '--to_datetime', '--to-datetime',
        dest='to_datetime',
        required=True,
        help='End datetime (YYYY-MM-DD HH:MM:SS)'
    )
    parser.add_argument(
        '--granularity',
        required=True,
        choices=['30m', '1day'],
        help='Aggregation granularity (30m or 1day)'
    )
    parser.add_argument(
        '--dimensions',
        required=True,
        help='Dimensions to group by (user, app, or user,app)'
    )
    parser.add_argument(
        '--user',
        help='Filter by user(s), comma-separated'
    )
    parser.add_argument(
        '--app',
        help='Filter by app(s), comma-separated'
    )
    parser.add_argument(
        '--output',
        default='result.csv',
        help='Output filename (default: result.csv)'
    )
    
    args = parser.parse_args()
    
    # Parse arguments
    from_dt = parse_datetime(args.from_datetime)
    to_dt = parse_datetime(args.to_datetime)
    
    # Parse dimensions
    dimensions = [d.strip() for d in args.dimensions.split(',')]
    valid_dims = {'user', 'app'}
    for dim in dimensions:
        if dim not in valid_dims:
            parser.error(f"Invalid dimension: {dim}. Must be 'user', 'app', or 'user,app'")
    
    # Parse filters
    user_filter = [u.strip() for u in args.user.split(',')] if args.user else None
    app_filter = [a.strip() for a in args.app.split(',')] if args.app else None
    
    print(f"\nQuerying logs from {from_dt} to {to_dt}")
    print(f"Granularity: {args.granularity}")
    print(f"Dimensions: {dimensions}")
    if user_filter:
        print(f"User filter: {user_filter}")
    if app_filter:
        print(f"App filter: {app_filter}")
    print()
    
    # Time reading and aggregation with resource tracking
    read_start = time.perf_counter()
    read_res_before = get_resource_usage()
    aggregated = aggregate_data(from_dt, to_dt, args.granularity, dimensions, user_filter, app_filter)
    read_res_after = get_resource_usage()
    read_time = time.perf_counter() - read_start
    read_resources = calc_resource_diff(read_res_before, read_res_after)
    
    print(f"Found {len(aggregated)} aggregated entries")
    
    # Write results with resource tracking
    write_time = 0
    write_resources = None
    if aggregated:
        write_start = time.perf_counter()
        write_res_before = get_resource_usage()
        write_results(aggregated, dimensions, args.output)
        write_res_after = get_resource_usage()
        write_time = time.perf_counter() - write_start
        write_resources = calc_resource_diff(write_res_before, write_res_after)
    else:
        print("No data found matching criteria.")
    
    # Performance summary
    total_time = time.perf_counter() - start_time
    final_resources = get_resource_usage()
    
    print(f"\n{'='*70}")
    print("PERFORMANCE METRICS")
    print(f"{'='*70}")
    print(f"{'Phase':<30} {'Time':>10} {'CPU User':>10} {'CPU Sys':>10} {'IO R':>6} {'IO W':>6}")
    print(f"{'-'*70}")
    print(f"{'Data reading & aggregation':<30} {read_time:>9.4f}s {read_resources['cpu_user']:>9.4f}s {read_resources['cpu_sys']:>9.4f}s {read_resources['io_read']:>6} {read_resources['io_write']:>6}")
    if write_resources:
        print(f"{'Writing results':<30} {write_time:>9.4f}s {write_resources['cpu_user']:>9.4f}s {write_resources['cpu_sys']:>9.4f}s {write_resources['io_read']:>6} {write_resources['io_write']:>6}")
    print(f"{'-'*70}")
    print(f"{'TOTAL':<30} {total_time:>9.4f}s")
    print(f"{'='*70}")
    print(f"Peak Memory Usage: {final_resources['max_rss'] / 1024:.2f} MB")
    print(f"{'='*70}")

if __name__ == '__main__':
    main()
