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
package com.datasqrl.plan.index;

import com.datasqrl.calcite.SqrlRexUtil;
import com.datasqrl.plan.index.IndexSelector.NamedTable;
import java.util.List;
import org.apache.calcite.rex.RexNode;

/**
 * A filter conjunct that would be indexable if its column stood alone: the database cannot use an
 * index on the column because the filter wraps it in an expression.
 *
 * @param table the table the filter applies to
 * @param reason the kind of expression that wraps the columns
 * @param expression the expression that wraps the columns
 * @param condition the filter conjunct
 */
public record UnindexableFilter(
    NamedTable table, Reason reason, RexNode expression, RexNode condition) {

  private static final String IN_FLINK_TABLE =
      "in a table that Flink computes (`/*+engine(flink) */`) and filter on that column.";

  public enum Reason {
    /** Arithmetic on a single column, like {@code col + 1 = :p} */
    ARITHMETIC,
    /** A type conversion of a single column, like {@code CAST(col AS STRING) = :p} */
    CAST,
    /** Any other expression, like {@code LOWER(col) = :p} */
    EXPRESSION
  }

  /**
   * @param functionName the name of the table function whose query contains the filter
   * @param parameterNames the names of the table function parameters, by parameter index
   */
  public String toWarning(String functionName, List<String> parameterNames) {
    var renderer = new ExpressionRenderer(columnNames(), parameterNames);
    return ("The filter `%s` in `%s` cannot use an index on table `%s` because %s wrapped in the"
            + " expression `%s`. %s")
        .formatted(
            renderer.render(condition),
            functionName,
            table.getTableName(),
            wrappedColumns(),
            renderer.render(expression),
            remedy());
  }

  /** Like {@code column `a` is} or {@code columns `a`, `b` are}. */
  private String wrappedColumns() {
    var columns = quotedColumnNames();
    return columns.size() == 1
        ? "column " + columns.get(0) + " is"
        : "columns " + String.join(", ", columns) + " are";
  }

  private String remedy() {
    var columns = String.join(", ", quotedColumnNames());
    return switch (reason) {
      case ARITHMETIC ->
          "Move the arithmetic to the other side of the comparison so that %s stands alone."
              .formatted(columns);
      case CAST ->
          ("Compare %s with a value of its type `%s`, such as a parameter of that type, or compute"
                  + " the converted value %s")
              .formatted(columns, columnTypeName(), IN_FLINK_TABLE);
      case EXPRESSION -> "Compute the expression " + IN_FLINK_TABLE;
    };
  }

  private List<Integer> columns() {
    return SqrlRexUtil.findAllInputRefs(List.of(expression)).stream().sorted().toList();
  }

  private List<String> quotedColumnNames() {
    return columns().stream().map(column -> "`" + columnNames().get(column) + "`").toList();
  }

  private List<String> columnNames() {
    return table.getAnalysis().getRowType().getFieldNames();
  }

  /** The type of the single column that a {@link Reason#CAST} converts. */
  private String columnTypeName() {
    var column = columns().get(0);
    return ExpressionRenderer.typeName(
        table.getAnalysis().getRowType().getFieldList().get(column).getType());
  }
}
