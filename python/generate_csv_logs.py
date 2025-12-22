
import argparse
import csv
import random
import os
import time
import resource
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from collections import defaultdict

# Configuration
USERS = [f'user{i}' for i in range(1, 100)]  # user1 to user99
APPS = ['facebook', 'twitter', 'youtube', 'instagram', 'tiktok', 'whatsapp', 'telegram', 'snapchat']

# Thread-local random generators for better performance
thread_local = threading.local()

def get_random():
    """Get thread-local random instance for better performance."""
    if not hasattr(thread_local, 'random'):
        thread_local.random = random.Random()
    return thread_local.random

def get_resource_usage():
    """Get current resource usage (CPU time, memory, I/O)."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        'user_time': usage.ru_utime,
        'sys_time': usage.ru_stime,
        'max_rss': usage.ru_maxrss,
        'io_read': usage.ru_inblock,
        'io_write': usage.ru_oublock
    }

def calc_resource_diff(before, after):
    """Calculate the difference in resource usage."""
    return {
        'cpu_user': after['user_time'] - before['user_time'],
        'cpu_sys': after['sys_time'] - before['sys_time'],
        'memory_kb': after['max_rss'],
        'io_read': after['io_read'] - before['io_read'],
        'io_write': after['io_write'] - before['io_write']
    }

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

def generate_random_timestamp(date_str, rng):
    """Generate a random timestamp for a given date."""
    hour = rng.randint(0, 23)
    minute = rng.randint(0, 59)
    second = rng.randint(0, 59)
    return f"{date_str} {hour:02d}:{minute:02d}:{second:02d}"

def generate_log_batch(date_str, batch_size):
    """Generate a batch of logs for a given date."""
    rng = get_random()
    logs = []
    
    for _ in range(batch_size):
        user = rng.choice(USERS)
        app = rng.choice(APPS)
        timestamp = generate_random_timestamp(date_str, rng)
        metrics = [rng.randint(1, 1000) for _ in range(9)]
        
        logs.append([timestamp, user, app] + metrics)
    
    return logs

def generate_logs_for_day_threaded(date_str, num_logs, num_threads, batch_size):
    """Generate logs for a single day using multiple threads."""
    all_logs = []
    batches_per_thread = (num_logs + batch_size - 1) // batch_size
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        remaining = num_logs
        
        while remaining > 0:
            current_batch = min(batch_size, remaining)
            futures.append(executor.submit(generate_log_batch, date_str, current_batch))
            remaining -= current_batch
        
        for future in as_completed(futures):
            all_logs.extend(future.result())
    
    # Sort by user for optimized reading
    all_logs.sort(key=lambda x: x[1])  # Sort by user (index 1)
    
    return all_logs

def save_logs_to_csv(logs, filepath):
    """Save logs to CSV file efficiently."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, 'w', newline='', buffering=65536) as f:
        writer = csv.writer(f)
        writer.writerows(logs)
    
    return len(logs)

def format_time(seconds):
    """Format seconds to human-readable time."""
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        mins = int(seconds // 60)
        secs = seconds % 60
        return f"{mins}m {secs:.1f}s"
    else:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours}h {mins}m"

def print_progress(current, total, start_time, prefix="Progress"):
    """Print progress bar with ETA."""
    pct = (current / total) * 100
    elapsed = time.perf_counter() - start_time
    if current > 0:
        eta = (elapsed / current) * (total - current)
        eta_str = format_time(eta)
    else:
        eta_str = "calculating..."
    
    bar_len = 40
    filled = int(bar_len * current / total)
    bar = '█' * filled + '░' * (bar_len - filled)
    
    print(f"\r{prefix}: [{bar}] {pct:5.1f}% ({current}/{total}) ETA: {eta_str}    ", end='', flush=True)

def main():
    parser = argparse.ArgumentParser(
        description='Generate sample CSV log files with multi-threading support.',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--days', type=int, default=90,
                        help='Number of days to generate (default: 30)')
    parser.add_argument('--logs-per-day', type=int, default=50000,
                        help='Number of logs per day (default: 50000)')
    parser.add_argument('--threads', type=int, default=4,
                        help='Number of worker threads (default: 4)')
    parser.add_argument('--batch-size', type=int, default=10000,
                        help='Logs per batch per thread (default: 10000)')
    parser.add_argument('--start-date', type=str, default='2025-01-01',
                        help='Start date YYYY-MM-DD (default: 2025-01-01)')
    parser.add_argument('--output-dir', type=str, default=None,
                        help='Output directory (default: ./csv_logs)')
    
    args = parser.parse_args()
    
    # Setup directories
    base_dir = os.path.dirname(os.path.abspath(__file__))
    logs_dir = args.output_dir or os.path.join(base_dir, 'csv_logs')
    
    # Parse start date
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    except ValueError:
        print(f"Error: Invalid date format '{args.start_date}'. Use YYYY-MM-DD")
        return
    
    # Print configuration
    print("=" * 70)
    print("CSV LOG GENERATOR - Configuration")
    print("=" * 70)
    print(f"  Days to generate:    {args.days}")
    print(f"  Logs per day:        {args.logs_per_day:,}")
    print(f"  Total logs:          {args.days * args.logs_per_day:,}")
    print(f"  Worker threads:      {args.threads}")
    print(f"  Batch size:          {args.batch_size:,}")
    print(f"  Start date:          {args.start_date}")
    print(f"  Output directory:    {logs_dir}")
    print("=" * 70)
    print()
    
    # Start performance tracking
    overall_start = time.perf_counter()
    overall_res_before = get_resource_usage()
    
    # Track per-phase metrics
    gen_time_total = 0
    write_time_total = 0
    total_logs_generated = 0
    
    print("Generating logs...")
    gen_start = time.perf_counter()
    
    for day_offset in range(args.days):
        current_date = start_date + timedelta(days=day_offset)
        date_str = current_date.strftime('%Y-%m-%d')
        
        # Generate logs for this day
        day_gen_start = time.perf_counter()
        logs = generate_logs_for_day_threaded(
            date_str, 
            args.logs_per_day, 
            args.threads, 
            args.batch_size
        )
        gen_time_total += time.perf_counter() - day_gen_start
        
        # Save to file
        filepath = os.path.join(logs_dir, f'{date_str}.log.csv')
        day_write_start = time.perf_counter()
        count = save_logs_to_csv(logs, filepath)
        write_time_total += time.perf_counter() - day_write_start
        
        total_logs_generated += count
        
        # Print progress
        print_progress(day_offset + 1, args.days, gen_start)
    
    print()  # New line after progress bar
    
    # Calculate final metrics
    overall_time = time.perf_counter() - overall_start
    overall_res_after = get_resource_usage()
    overall_resources = calc_resource_diff(overall_res_before, overall_res_after)
    
    # Calculate throughput
    logs_per_second = total_logs_generated / overall_time if overall_time > 0 else 0
    
    # Calculate averages
    avg_time_per_day = overall_time / args.days if args.days > 0 else 0
    avg_gen_time_per_day = gen_time_total / args.days if args.days > 0 else 0
    avg_write_time_per_day = write_time_total / args.days if args.days > 0 else 0
    
    # Print performance summary
    print()
    print("=" * 70)
    print("GENERATION COMPLETE")
    print("=" * 70)
    print(f"  Total files created: {args.days}")
    print(f"  Total logs:          {total_logs_generated:,}")
    print(f"  Output directory:    {logs_dir}")
    print()
    print("=" * 70)
    print("PERFORMANCE METRICS")
    print("=" * 70)
    print(f"{'Phase':<25} {'Time Total':>15} {'Time Avg/Day':>15} {'Throughput':>12}")
    print("-" * 70)
    print(f"{'Log generation':<25} {format_time(gen_time_total):>15} {format_time(avg_gen_time_per_day):>15} {total_logs_generated/gen_time_total:>9,.0f} l/s")
    print(f"{'File writing':<25} {format_time(write_time_total):>15} {format_time(avg_write_time_per_day):>15} {total_logs_generated/write_time_total:>9,.0f} l/s")
    print("-" * 70)
    print(f"{'TOTAL':<25} {format_time(overall_time):>15} {format_time(avg_time_per_day):>15} {logs_per_second:>9,.0f} l/s")
    print("=" * 70)
    print()
    print("TIME SUMMARY")
    print("-" * 70)
    print(f"  Total Time:          {format_time(overall_time)}")
    print(f"  Avg Time per Day:    {format_time(avg_time_per_day)}")
    print(f"  Avg Time per Log:    {overall_time / total_logs_generated * 1000:.4f} ms")
    
    # Get CPU info
    cpu_info = get_cpu_info()
    cpu_percent = calc_cpu_percent(cpu_times_before, cpu_times_after) if 'cpu_times_before' in dir() else None
    
    print()
    print("CPU METRICS")
    print("-" * 70)
    print(f"  CPU Model:           {cpu_info.get('model', 'Unknown')}")
    print(f"  CPU Cores:           {cpu_info['cores']} (using {args.threads} threads)")
    if cpu_info.get('freq_mhz'):
        print(f"  CPU Frequency:       {cpu_info['freq_mhz']:.0f} MHz ({cpu_info['freq_mhz']/1000:.2f} GHz)")
    print(f"  CPU Efficiency:      {((overall_resources['cpu_user'] + overall_resources['cpu_sys']) / overall_time) * 100:.1f}%")
    print()
    print("MEMORY & I/O")
    print("-" * 70)
    print(f"  Peak Memory:         {overall_resources['memory_kb'] / 1024:.2f} MB")
    print(f"  I/O Read Ops:        {overall_resources['io_read']}")
    print(f"  I/O Write Ops:       {overall_resources['io_write']}")
    print("=" * 70)

if __name__ == '__main__':
    main()
