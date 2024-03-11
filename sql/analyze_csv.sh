#!/bin/bash

# Check if a file name is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 filename"
    exit 1
fi

filename=$1

# Check if the file exists
if [ ! -f "$filename" ]; then
    echo "File not found!"
    exit 1
fi

# Determine the number of columns in the CSV
num_columns=$(head -1 $filename | awk -F, '{print NF}')
echo "The file has $num_columns columns."

# Count unique values in each column
for i in $(seq 1 $num_columns); do
    # Extracting each column with cut, sorting, getting unique values, and counting
    unique_values=$(cut -d',' -f$i $filename | sort | uniq | wc -l)
    echo "Column $i has $unique_values unique values."
done


