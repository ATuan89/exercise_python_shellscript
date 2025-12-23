
set -e

# Check argument
if [ -z "$1" ]; then
    echo "Usage: $0 YYYY-MM-DD"
    echo "Aggregates user-application and user-device CSV files for a given date"
    exit 1
fi

DATE="$1"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/sample_data"
OUTPUT_DIR="${SCRIPT_DIR}/output"

# Validate date format
if ! [[ "$DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "Error: Invalid date format. Use YYYY-MM-DD"
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo "Processing data for date: $DATE"
echo "=============================================="

# Check if data files exist
APP_FILES=$(find "$DATA_DIR" -name "user.application.${DATE}*.csv" 2>/dev/null | wc -l)
DEVICE_FILES=$(find "$DATA_DIR" -name "user.device.${DATE}*.csv" 2>/dev/null | wc -l)

if [ "$APP_FILES" -eq 0 ] && [ "$DEVICE_FILES" -eq 0 ]; then
    echo "Warning: No files found for date $DATE in $DATA_DIR"
    echo "Run ./generate_sample_data.sh $DATE first to create sample data"
    exit 1
fi

echo "Found $APP_FILES application files and $DEVICE_FILES device files"
echo ""

start_time=$(date +%s.%N)

# 1. Create app_users.csv: application,number of unique users
echo "Generating app_users.csv..."
echo "application,unique_users" > "$OUTPUT_DIR/app_users.csv"

cat "$DATA_DIR"/user.application.${DATE}*.csv 2>/dev/null | \
    awk -F',' '{print $2","$1}' | \
    sort | \
    uniq | \
    awk -F',' '{count[$1]++} END {for (app in count) print app","count[app]}' | \
    sort >> "$OUTPUT_DIR/app_users.csv"

# 2. Create device_users.csv: device,number of unique users
echo "Generating device_users.csv..."
echo "device,unique_users" > "$OUTPUT_DIR/device_users.csv"

cat "$DATA_DIR"/user.device.${DATE}*.csv 2>/dev/null | \
    awk -F',' '{print $2","$1}' | \
    sort | \
    uniq | \
    awk -F',' '{count[$1]++} END {for (dev in count) print dev","count[dev]}' | \
    sort >> "$OUTPUT_DIR/device_users.csv"

# 3. Create app_device_combinations.csv: all unique application,device combinations
echo "Generating app_device_combinations.csv..."
echo "application,device" > "$OUTPUT_DIR/app_device_combinations.csv"

# Get unique apps and devices, then create all combinations
# First, get unique apps from user-app data
TEMP_APPS=$(mktemp)
TEMP_DEVICES=$(mktemp)

cat "$DATA_DIR"/user.application.${DATE}*.csv 2>/dev/null | \
    cut -d',' -f2 | \
    sort -u > "$TEMP_APPS"

cat "$DATA_DIR"/user.device.${DATE}*.csv 2>/dev/null | \
    cut -d',' -f2 | \
    sort -u > "$TEMP_DEVICES"

# Create all combinations using nested loops
while IFS= read -r app; do
    while IFS= read -r device; do
        echo "$app,$device"
    done < "$TEMP_DEVICES"
done < "$TEMP_APPS" >> "$OUTPUT_DIR/app_device_combinations.csv"

# Cleanup temp files
rm -f "$TEMP_APPS" "$TEMP_DEVICES"

end_time=$(date +%s.%N)
elapsed=$(echo "$end_time - $start_time" | bc)

echo ""
echo "=============================================="
echo "Completed in ${elapsed}s"
echo ""
echo "Output files:"
echo "  - $OUTPUT_DIR/app_users.csv"
echo "  - $OUTPUT_DIR/device_users.csv"
echo "  - $OUTPUT_DIR/app_device_combinations.csv"
echo ""

# Show preview
echo "Preview of app_users.csv:"
head -5 "$OUTPUT_DIR/app_users.csv"
echo ""
echo "Preview of device_users.csv:"
head -5 "$OUTPUT_DIR/device_users.csv"
echo ""
echo "Preview of app_device_combinations.csv:"
head -5 "$OUTPUT_DIR/app_device_combinations.csv"
