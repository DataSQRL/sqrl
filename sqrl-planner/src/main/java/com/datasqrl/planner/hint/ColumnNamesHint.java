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
import com.datasqrl.planner.analyzer.ColumnReference;
import com.datasqrl.planner.analyzer.SQRLLogicalPlanAnalyzer;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SqrlHint;
import com.datasqrl.planner.parser.StatementParserException;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

/**
 * A hint that has column names. Those get validated by {@link SQRLLogicalPlanAnalyzer} and indexes
 * are added. Hence, it can be assumed that the hints have resolved indexes after planning. The
 * names resolve as described in {@link ColumnReference}.
 */
public abstract class ColumnNamesHint extends PlannerHint {

  /** The column names as they are written in the hint */
  private final List<ColumnReference> references;

  private List<String> colNames;
  private List<Integer> colIndexes;

  protected ColumnNamesHint(ParsedObject<SqrlHint> source, Type type, List<String> columnNames) {
    super(source, type);
    this.references = columnNames.stream().map(ColumnReference::parse).toList();
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
   * Resolves the column names that are written in the hint against the columns of the table, see
   * {@link ColumnReference}, and replaces them with the names and indexes of those columns.
   *
   * @param rowType the row type of the table
   */
  public void validateAndUpdate(RelDataType rowType) {
    var columns = references.stream().map(reference -> resolve(reference, rowType)).toList();
    colNames = columns.stream().map(RelDataTypeField::getName).toList();
    colIndexes = columns.stream().map(RelDataTypeField::getIndex).toList();
  }

  private RelDataTypeField resolve(ColumnReference reference, RelDataType rowType) {
    var matches = reference.matchingColumns(rowType);
    if (matches.isEmpty()) {
      throw new StatementParserException(
          ErrorLabel.GENERIC,
          getSource().getFileLocation(),
          "%s hint reference column [%s] that does not exist in table",
          getName(),
          reference);
    }

    if (matches.size() > 1) {
      throw new StatementParserException(
          ErrorLabel.GENERIC,
          getSource().getFileLocation(),
          "%s hint references column [%s] which matches columns that differ only in case: write"
              + " %s to match exactly one",
          getName(),
          reference,
          ColumnReference.alternatives(matches));
    }

    return matches.get(0);
  }
}
