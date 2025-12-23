
set -e

# Check argument
if [ -z "$1" ]; then
    echo "Usage: $0 <path>"
    echo "Finds directories with .git using more than 1GB"
    exit 1
fi

TARGET_PATH="$1"
ONE_GB=$((1024 * 1024 * 1024))  # 1GB in bytes

# Check if path exists
if [ ! -d "$TARGET_PATH" ]; then
    echo "Error: '$TARGET_PATH' is not a valid directory"
    exit 1
fi

echo "Scanning for .git directories under: $TARGET_PATH"
echo "=============================================="
echo ""

found_count=0

# Find all .git directories
while IFS= read -r git_dir; do
    # Get parent directory (the actual project directory)
    project_dir=$(dirname "$git_dir")
    
    # Measure time for this directory
    start_time=$(date +%s.%N)
    
    # Get directory size in bytes
    size_bytes=$(du -sb "$project_dir" 2>/dev/null | cut -f1)
    
    end_time=$(date +%s.%N)
    elapsed=$(echo "$end_time - $start_time" | bc)
    
    # Check if size > 1GB
    if [ "$size_bytes" -gt "$ONE_GB" ]; then
        # Convert to human readable
        size_human=$(du -sh "$project_dir" 2>/dev/null | cut -f1)
        
        echo "Directory: $project_dir"
        echo "  Size: $size_human ($size_bytes bytes)"
        echo "  Time: ${elapsed}s"
        echo ""
        
        ((found_count++)) || true
    fi
done < <(find "$TARGET_PATH" -type d -name ".git" 2>/dev/null)

echo "=============================================="
echo "Found $found_count directories with .git > 1GB"
