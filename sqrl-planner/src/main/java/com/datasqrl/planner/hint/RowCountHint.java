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
 * is empty
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
      var options = source.get().getOptions();
      if (options.isEmpty()) {
        throw new StatementParserException(
            ErrorLabel.GENERIC,
            source.getFileLocation(),
            "row_count hint must have at least one number argument");
      }
      var rowCountStr = options.get(options.size() - 1);
      double rowCount;
      try {
        rowCount = Double.parseDouble(rowCountStr);
      } catch (NumberFormatException e) {
        throw new StatementParserException(
            ErrorLabel.GENERIC,
            source.getFileLocation(),
            "row_count must be a valid number: %s (%s).",
            rowCountStr,
            e.getMessage());
      }
      if (rowCount <= 0) {
        throw new StatementParserException(
            ErrorLabel.GENERIC,
            source.getFileLocation(),
            "row_count must be a positive number: %s.",
            rowCountStr);
      }
      var columnNames = options.subList(0, options.size() - 1);
      return new RowCountHint(source, columnNames, rowCount);
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }
}
