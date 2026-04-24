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
package com.datasqrl.planner.hint;

import com.datasqrl.error.ErrorLabel;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SqrlHint;
import com.datasqrl.planner.parser.StatementParserException;
import com.google.auto.service.AutoService;
import java.util.List;
import lombok.Getter;

/**
 * Annotates a table with a distinct row count for a set columns - or total row count if column list
 * is empty. Examples: - row_count(1e6): this tables has approximately a million rows -
 * row_count(col1, col2, 2e3): there are 2000 unique combinations of (col1, col2) values in this
 * table
 */
public class RowCountHint extends ColumnNamesHint {

  public static final String HINT_NAME = "row_count";

  @Getter private final double rowCount;

  protected RowCountHint(ParsedObject<SqrlHint> source, List<String> columns, double rowCount) {
    super(source, Type.DAG, columns);
    this.rowCount = rowCount;
  }

  @AutoService(Factory.class)
  public static class RowCountHintFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      var args = source.get().getOptions();
      if (args.isEmpty()) {
        throw new StatementParserException(
            ErrorLabel.GENERIC,
            source.getFileLocation(),
            "The row_count hint must have at least one numeric argument. If column names are present, the numeric count must appear at the end.");
      }

      var rowCount = parseRowCountStr(source, args);
      var columnNames = args.subList(0, args.size() - 1);

      return new RowCountHint(source, columnNames, rowCount);
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }

    private static double parseRowCountStr(ParsedObject<SqrlHint> source, List<String> args) {
      var rowCountStr = args.get(args.size() - 1);
      double rowCount;

      try {
        rowCount = Double.parseDouble(rowCountStr);
      } catch (NumberFormatException e) {
        throw new StatementParserException(
            ErrorLabel.GENERIC,
            source.getFileLocation(),
            "The row_count must be a valid number. Got: %s",
            rowCountStr);
      }

      if (rowCount <= 0) {
        throw new StatementParserException(
            ErrorLabel.GENERIC,
            source.getFileLocation(),
            "The row_count must be a positive number. Got: %s",
            rowCountStr);
      }

      return rowCount;
    }
  }
}
