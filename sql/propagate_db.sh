#!/bin/bash

# Define a function to run commands and log their time and additional information
run_and_log_time() {
    # Command to execute
    local command="$1"
    # Log file name
    local logfile="$2"

    # Get the current date and time in ISO 8601 format
    local start_time=$(date --iso-8601=seconds)

    # Print start time and command to both log file and console
    echo "$(date --iso-8601=seconds): Starting $command" | tee -a "$logfile"

    # Run the command, unbuffered, with output to log file and terminal, adding timestamps
    echo "Running $command..." | tee -a "$logfile"
    { time unbuffer python3 manage.py $command 2>&1; } | while IFS= read -r line; do
        echo "$(date --iso-8601=seconds): $line"
    done | tee -a "$logfile"

    # Get the end date and time in ISO 8601 format
    local end_time=$(date --iso-8601=seconds)

    # Echo the end time to log file and console
    echo "$(date --iso-8601=seconds): Finished $command" | tee -a "$logfile"

    # Echo the duration to log file and console
    local duration=$(($(date +%s -d "$end_time") - $(date +%s -d "$start_time")))
    echo "Time taken for $command: $duration seconds" | tee -a "$logfile"
}

run_and_log_time "--insert_users_and_categories" "users_and_categories_log.txt"
run_and_log_time "--insert_products" "products_log.txt"
run_and_log_time "--insert_events" "events_log.txt"
