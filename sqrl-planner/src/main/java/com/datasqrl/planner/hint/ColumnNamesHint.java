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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.calcite.rel.type.RelDataTypeField;

/**
 * A hint that has column names Those get validated by {@link
 * com.datasqrl.planner.analyzer.SQRLLogicalPlanAnalyzer} and indexes are added. Hence, it can be
 * assumed that the hints have resolved indexes after planning.
 */
public abstract class ColumnNamesHint extends PlannerHint {

  private List<String> colNames;
  private List<Integer> colIndexes;

  protected ColumnNamesHint(ParsedObject<SqrlHint> source, Type type, List<String> columnNames) {
    super(source, type);
    this.colNames = columnNames;
    this.colIndexes = null;
  }

  public List<String> getColumnNames() {
    return colNames;
  }

  public List<Integer> getColumnIndexes() {
    return colIndexes;
  }

  /**
   * Updates the column names with the normalized names as they are defined on the table
   *
   * @param columnNames
   * @param columnIndexes
   */
  private void updateColumns(List<String> columnNames, List<Integer> columnIndexes) {
    this.colNames = columnNames;
    this.colIndexes = columnIndexes;
  }

  public void validateAndUpdate(Function<String, RelDataTypeField> fieldByIndex) {
    // Validate column names in hints and map to indexes
    List<String> colNames = new ArrayList<>();
    List<Integer> colIndexes = new ArrayList<>();
    for (String colName : getColumnNames()) {
      var field = fieldByIndex.apply(colName);
      if (field == null) {
        throw new StatementParserException(
            ErrorLabel.GENERIC,
            getSource().getFileLocation(),
            "%s hint reference column [%s] that does not exist in table",
            getName(),
            colName);
      }
      colNames.add(field.getName());
      colIndexes.add(field.getIndex());
    }
    updateColumns(colNames, colIndexes);
  }
}
