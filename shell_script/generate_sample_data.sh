#!/bin/bash
#
# Generate Sample Data for Shell Script Exercises
# Creates sample CSV files for testing exercise2.sh and exercise3.sh
#
# Usage: ./generate_sample_data.sh [YYYY-MM-DD]
#   If no date provided, generates data for multiple days

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/sample_data"

# Sample data
USERS=("user1" "user2" "user3" "user4" "user5" "user6" "user7" "user8" "user9")
APPS=("app1" "app2" "app3" "app4" "app5")
DEVICES=("device1" "device2" "device3" "device4" "device5")

mkdir -p "$DATA_DIR"

generate_30min_files() {
    local date=$1
    echo "Generating 48 x 30-min interval files for $date..."
    
    # Generate 48 files (24 hours * 2 = 48 half-hour intervals)
    for hour in $(seq -w 0 23); do
        for min in 00 30; do
            local filename="user.application.${date}-${hour}-${min}.csv"
            local devicefile="user.device.${date}-${hour}-${min}.csv"
            
            # Generate random app entries
            for i in $(seq 1 $((RANDOM % 10 + 5))); do
                user=${USERS[$RANDOM % ${#USERS[@]}]}
                app=${APPS[$RANDOM % ${#APPS[@]}]}
                echo "$user,$app" >> "$DATA_DIR/$filename"
            done
            
            # Generate random device entries
            for i in $(seq 1 $((RANDOM % 10 + 5))); do
                user=${USERS[$RANDOM % ${#USERS[@]}]}
                device=${DEVICES[$RANDOM % ${#DEVICES[@]}]}
                echo "$user,$device" >> "$DATA_DIR/$devicefile"
            done
        done
    done
}

generate_daily_file() {
    local date=$1
    echo "Generating daily file for $date..."
    
    local filename="user.application.${date}.csv"
    
    # Generate random entries
    for i in $(seq 1 $((RANDOM % 20 + 10))); do
        user=${USERS[$RANDOM % ${#USERS[@]}]}
        app=${APPS[$RANDOM % ${#APPS[@]}]}
        echo "$user,$app" >> "$DATA_DIR/$filename"
    done
}

# Clean existing data
rm -rf "$DATA_DIR"/*.csv 2>/dev/null || true

echo "Generating sample data in: $DATA_DIR"
echo "=============================================="

if [ -n "$1" ]; then
    # Generate for specific date
    generate_30min_files "$1"
    generate_daily_file "$1"
else
    # Generate for multiple dates (for exercise 3)
    DATES=("2020-01-16" "2020-01-17" "2020-01-18" "2020-01-19" "2020-01-20")
    
    for date in "${DATES[@]}"; do
        generate_30min_files "$date"
        generate_daily_file "$date"
    done
fi

echo ""
echo "=============================================="
echo "Sample data generated!"
echo ""
echo "Files created:"
ls -la "$DATA_DIR" | head -20
echo "..."
echo "Total files: $(ls "$DATA_DIR" | wc -l)"
