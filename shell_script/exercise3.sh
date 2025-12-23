#!/bin/bash
# Exercise 3
# Prints list of users who used the provided app every day up to the provided date.

if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: $0 <YYYY-MM-DD> <application>"
    exit 1
fi

TARGET_DATE="$1"
TARGET_APP="$2"
DATA_DIR="shell_script/test_data/ex3"
if [ ! -d "$DATA_DIR" ]; then DATA_DIR="."; fi

RESULTS_DIR="shell_script/results"
mkdir -p "$RESULTS_DIR"
OUTPUT_FILE="$RESULTS_DIR/exercise3_users.txt"

CANDIDATES=$(mktemp)
CURRENT_DAY_USERS=$(mktemp)
TEMP_INTERSECTION=$(mktemp)

trap "rm -f $CANDIDATES $CURRENT_DAY_USERS $TEMP_INTERSECTION" EXIT

FILES=$(find "$DATA_DIR" -maxdepth 1 -name "user.application.*.csv" | sort)
FIRST_FILE=true

for file in $FILES; do
    fname=$(basename "$file")
    fdate=$(echo "$fname" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}')
    
    if [[ "$fdate" > "$TARGET_DATE" ]]; then break; fi
    
    awk -F, -v app="$TARGET_APP" '$2 == app {print $1}' "$file" | sort -u > "$CURRENT_DAY_USERS"
    
    if [ "$FIRST_FILE" = true ]; then
        cp "$CURRENT_DAY_USERS" "$CANDIDATES"
        FIRST_FILE=false
    else
        comm -12 "$CANDIDATES" "$CURRENT_DAY_USERS" > "$TEMP_INTERSECTION"
        mv "$TEMP_INTERSECTION" "$CANDIDATES"
        if [ ! -s "$CANDIDATES" ]; then break; fi
    fi
done

{
    echo "Users using $TARGET_APP every day up to $TARGET_DATE:"
    if [ -s "$CANDIDATES" ]; then
        cat "$CANDIDATES"
    else
        echo "No users found."
    fi
} | tee "$OUTPUT_FILE"

echo "Results saved to $OUTPUT_FILE"
