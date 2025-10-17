#!/bin/bash
#
# Copyright © 2021 DataSQRL (contact@datasqrl.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


# Script to update configuration-default.md with the latest default-package.json content
# This script replaces everything between ```json and ``` with the contents of default-package.json

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MD_FILE="$SCRIPT_DIR/configuration-default.md"
JSON_FILE="$SCRIPT_DIR/../../sqrl-planner/src/main/resources/default-package.json"

# Check if files exist
if [ ! -f "$MD_FILE" ]; then
    echo "Error: $MD_FILE not found"
    exit 1
fi

if [ ! -f "$JSON_FILE" ]; then
    echo "Error: $JSON_FILE not found"
    exit 1
fi

echo "Updating $MD_FILE with contents from $JSON_FILE..."

# Create temporary files
TEMP_FILE=$(mktemp)
BEFORE_JSON=$(mktemp)
AFTER_JSON=$(mktemp)

# Extract the part before ```json
sed -n '1,/^```json$/p' "$MD_FILE" > "$BEFORE_JSON"

# Extract the part after the closing ```
sed -n '/^```$/,$p' "$MD_FILE" | tail -n +2 > "$AFTER_JSON"

# Combine: before + json content + after
cat "$BEFORE_JSON" > "$TEMP_FILE"
cat "$JSON_FILE" >> "$TEMP_FILE"
echo '```' >> "$TEMP_FILE"
cat "$AFTER_JSON" >> "$TEMP_FILE"

# Replace the original file
mv "$TEMP_FILE" "$MD_FILE"

# Clean up temporary files
rm -f "$BEFORE_JSON" "$AFTER_JSON"

echo "Successfully updated $MD_FILE with the latest default configuration"