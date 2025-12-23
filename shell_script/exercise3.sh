
set -e

# Check arguments
if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: $0 YYYY-MM-DD <application>"
    echo "Finds users who used <application> every day up to the given date"
    exit 1
fi

DATE="$1"
APP="$2"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/sample_data"

# Validate date format
if ! [[ "$DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "Error: Invalid date format. Use YYYY-MM-DD"
    exit 1
fi

echo "Finding users who used '$APP' every day up to $DATE"
echo "=============================================="

# Find all daily files up to the given date
TEMP_FILE=$(mktemp)
TEMP_COUNTS=$(mktemp)

# Count how many day files exist up to and including the target date
day_count=0
file_list=""

# Find all available daily files (only files matching exactly YYYY-MM-DD.csv, not YYYY-MM-DD-HH-MM.csv)
for file in "$DATA_DIR"/user.application.[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9].csv; do
    if [ ! -f "$file" ]; then
        continue
    fi
    
    # Extract date from filename
    filename=$(basename "$file")
    file_date="${filename#user.application.}"
    file_date="${file_date%.csv}"
    
    # Compare dates (file_date <= target date)
    if [[ "$file_date" < "$DATE" ]] || [[ "$file_date" == "$DATE" ]]; then
        ((day_count++)) || true
        file_list="$file_list $file"
    fi
done

if [ "$day_count" -eq 0 ]; then
    echo "No daily files found up to $DATE"
    echo "Make sure files exist with format: user.application.YYYY-MM-DD.csv"
    rm -f "$TEMP_FILE" "$TEMP_COUNTS"
    exit 1
fi

echo "Found $day_count daily files up to $DATE"
echo ""

start_time=$(date +%s.%N)

# For each file, extract users who used the specified app
for file in $file_list; do
    # Filter by app (column 2) and get unique users per file
    grep ",${APP}$" "$file" 2>/dev/null | cut -d',' -f1 | sort -u >> "$TEMP_FILE"
done

# Count occurrences of each user
sort "$TEMP_FILE" | uniq -c | sort -rn > "$TEMP_COUNTS"

# Find users who appear in ALL days (count == day_count)
echo "Users who used '$APP' every day (all $day_count days):"
echo "----------------------------------------------"

result_count=0
while read -r count user; do
    if [ "$count" -eq "$day_count" ]; then
        echo "  $user"
        ((result_count++))
    fi
done < "$TEMP_COUNTS"

end_time=$(date +%s.%N)
elapsed=$(echo "$end_time - $start_time" | bc)

# Cleanup
rm -f "$TEMP_FILE" "$TEMP_COUNTS"

echo ""
echo "=============================================="
echo "Found $result_count users using '$APP' every day"
echo "Completed in ${elapsed}s"
