#!/bin/bash
# Exercise 1
# Provide a shell script that takes a path as argument and
# • prints the size of each directory directly or nested under the path which (a) contains a .git directory (b) uses more than 1 GB
# • Prints the time taken for the check per directory found

if [ -z "$1" ]; then
    echo "Usage: $0 <path>"
    exit 1
fi

SEARCH_PATH="$1"
RESULTS_DIR="shell_script/results"
mkdir -p "$RESULTS_DIR"
OUTPUT_FILE="$RESULTS_DIR/exercise1_result.txt"

echo "Scanning $SEARCH_PATH..." | tee "$OUTPUT_FILE"
echo "-----------------------------------" | tee -a "$OUTPUT_FILE"

# Find all .git directories
find "$SEARCH_PATH" -name ".git" -type d 2>/dev/null | while read git_dir; do
    repo_dir=$(dirname "$git_dir")
    
    start_time=$(date +%s%N)
    # Using --apparent-size to detect sparse files from test data
    size_mb=$(du -sm --apparent-size "$repo_dir" | awk '{print $1}')
    end_time=$(date +%s%N)
    
    duration_ms=$(( (end_time - start_time) / 1000000 ))
    
    if [ "$size_mb" -gt 1024 ]; then
        {
            echo "Found: $repo_dir"
            echo "Size: $size_mb MB"
            echo "Time taken: ${duration_ms} ms"
            echo "-----------------------------------"
        } | tee -a "$OUTPUT_FILE"
    fi
done

echo "Results saved to $OUTPUT_FILE"
