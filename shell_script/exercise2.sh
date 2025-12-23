#!/bin/bash
# Exercise 2
# Given 48 csv files with columns "user","application" and "user","device"
# provide a shell script that takes a date YYYY-MM-DD as argument and
# • writes 1 csv file with columns "application","number of unique users"
# • writes 1 csv file with columns "device","number of unique users"
# • writes 1 csv file with columns "application","device" with all possible unique application and device combinations
# Assume input files are several GB each.

if [ -z "$1" ]; then
    echo "Usage: $0 <YYYY-MM-DD>"
    exit 1
fi

DATE="$1"
DATA_DIR="shell_script/test_data/ex2"
if [ ! -d "$DATA_DIR" ]; then DATA_DIR="."; fi

RESULTS_DIR="shell_script/results/"
mkdir -p "$RESULTS_DIR"

# Temp files
TMP_APPS=$(mktemp)
TMP_DEVICES=$(mktemp)
TMP_JOIN_APPS=$(mktemp)
TMP_JOIN_DEVICES=$(mktemp)

trap "rm -f $TMP_APPS $TMP_DEVICES $TMP_JOIN_APPS $TMP_JOIN_DEVICES" EXIT

echo "Processing data for $DATE..."

# 1. Aggregate Application data
find "$DATA_DIR" -name "user.application.$DATE-*.csv" -print0 | xargs -0 cat | sort -u > "$TMP_APPS"

# Output 1
OUT1="$RESULTS_DIR/apps_users.csv"
echo '"application","number of unique users"' > "$OUT1"
cut -d, -f2 "$TMP_APPS" | sort | uniq -c | awk '{print $2 "," $1}' >> "$OUT1"
echo "Created $OUT1"

# 2. Aggregate Device data
find "$DATA_DIR" -name "user.device.$DATE-*.csv" -print0 | xargs -0 cat | sort -u > "$TMP_DEVICES"

# Output 2
OUT2="$RESULTS_DIR/devices_users.csv"
echo '"device","number of unique users"' > "$OUT2"
cut -d, -f2 "$TMP_DEVICES" | sort | uniq -c | awk '{print $2 "," $1}' >> "$OUT2"
echo "Created $OUT2"

# 3. Application - Device combinations
sort -t, -k1,1 "$TMP_APPS" > "$TMP_JOIN_APPS"
sort -t, -k1,1 "$TMP_DEVICES" > "$TMP_JOIN_DEVICES"

OUT3="$RESULTS_DIR/apps_devices.csv"
echo '"application","device"' > "$OUT3"
join -t, -1 1 -2 1 -o 1.2,2.2 "$TMP_JOIN_APPS" "$TMP_JOIN_DEVICES" | sort -u >> "$OUT3"
echo "Created $OUT3"
