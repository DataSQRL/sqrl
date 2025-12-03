#!/bin/bash

for report_dir in $(find . -type d -path "*/target/surefire-reports" -o -path "*/target/failsafe-reports" 2>/dev/null); do
  if [ -d "$report_dir" ]; then
    for file in "$report_dir"/*.txt; do
      if [ -f "$file" ] && [[ ! "$file" =~ -output\.txt$ ]]; then
        test_name=$(basename "$file" .txt)

        echo ""
        echo "==== START: $test_name ===="
        echo "FILE: $file"
        echo ""
        cat "$file"

        output_file="${file%.txt}-output.txt"
        if [ -f "$output_file" ]; then
          echo ""
          echo "Console Output:"
          cat "$output_file"
        fi

        echo ""
        echo "==== END: $test_name ===="
        echo ""
      fi
    done
  fi
done

