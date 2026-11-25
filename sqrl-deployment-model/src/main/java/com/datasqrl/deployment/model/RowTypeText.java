/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.deployment.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Reads the text form of a Flink {@code ROW} type, for example {@code ROW<`id` INT, `cnt` BIGINT
 * NOT NULL>}. A compiled plan uses this form for an {@code outputType} that has no time attribute.
 */
final class RowTypeText {

  private static final String ROW_PREFIX = "ROW<";

  private RowTypeText() {}

  /**
   * Returns the column types in their order, without the column names and the column descriptions.
   * Returns empty when the text is not a {@code ROW} type. A nested type keeps its full text, also
   * its nested field names, because Flink compares the nested names.
   */
  static Optional<List<String>> columnTypes(String rowTypeText) {
    var rowTypeEnd = rowTypeText.lastIndexOf('>');
    if (!rowTypeText.startsWith(ROW_PREFIX) || rowTypeEnd < ROW_PREFIX.length()) {
      return Optional.empty();
    }
    var columnTypes = new ArrayList<String>();
    for (var columnText : topLevelColumns(rowTypeText.substring(ROW_PREFIX.length(), rowTypeEnd))) {
      var columnType = withoutColumnName(columnText.strip());
      if (columnType.isEmpty()) {
        return Optional.empty();
      }
      columnTypes.add(withoutColumnDescription(columnType.get()));
    }
    return Optional.of(columnTypes);
  }

  /**
   * Splits the columns at each comma that is outside of {@code <...>}, {@code (...)}, a quoted name
   * and a quoted description. {@code DECIMAL(10, 2)} and a nested {@code ROW} stay one column.
   */
  private static List<String> topLevelColumns(String columnsText) {
    var columns = new ArrayList<String>();
    var nestingDepth = 0;
    var insideName = false;
    var insideDescription = false;
    var columnStart = 0;
    for (var position = 0; position < columnsText.length(); position++) {
      var character = columnsText.charAt(position);
      if (character == '`' && !insideDescription) {
        insideName = !insideName;
      } else if (character == '\'' && !insideName) {
        insideDescription = !insideDescription;
      } else if (!insideName && !insideDescription) {
        if (character == '<' || character == '(') {
          nestingDepth++;
        } else if (character == '>' || character == ')') {
          nestingDepth--;
        } else if (character == ',' && nestingDepth == 0) {
          columns.add(columnsText.substring(columnStart, position));
          columnStart = position + 1;
        }
      }
    }
    if (!columnsText.isBlank()) {
      columns.add(columnsText.substring(columnStart));
    }
    return columns;
  }

  /** Removes the quoted name at the start of a column. A doubled backtick is part of the name. */
  private static Optional<String> withoutColumnName(String columnText) {
    if (!columnText.startsWith("`")) {
      return Optional.empty();
    }
    var position = 1;
    while (position < columnText.length()) {
      if (columnText.charAt(position) == '`') {
        if (position + 1 < columnText.length() && columnText.charAt(position + 1) == '`') {
          position += 2;
          continue;
        }
        return Optional.of(columnText.substring(position + 1).strip());
      }
      position++;
    }
    return Optional.empty();
  }

  /** Removes a quoted description at the end of a column, for example {@code INT 'the id'}. */
  private static String withoutColumnDescription(String columnType) {
    return columnType.replaceFirst("\\s+'(?:[^']|'')*'$", "");
  }
}
