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

import com.datasqrl.engine.stream.flink.sql.calcite.FlinkDialect;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;

/**
 * Renders an expression over the columns of a table as it is written in a SQRL script: with column
 * names, table function parameters as {@code :name}, and Flink type names.
 */
final class ExpressionRenderer extends SqlImplementor.SimpleContext {

  private final List<String> parameterNames;

  /**
   * @param columnNames the names of the table columns, by column index
   * @param parameterNames the names of the table function parameters, by parameter index
   */
  ExpressionRenderer(List<String> columnNames, List<String> parameterNames) {
    super(
        FlinkDialect.DEFAULT,
        index -> new SqlIdentifier(columnNames.get(index), SqlParserPos.ZERO));
    this.parameterNames = parameterNames;
  }

  /** The Flink SQL name of a type, like {@code STRING} or {@code TIMESTAMP_LTZ(3)}. */
  static String typeName(RelDataType type) {
    try {
      return FlinkTypeFactory.toLogicalType(type).copy(true).asSummaryString();
    } catch (RuntimeException e) {
      return type.getSqlTypeName().getName();
    }
  }

  String render(RexNode expression) {
    try {
      return toSql(null, expression)
          .toSqlString(
              config ->
                  config
                      .withDialect(FlinkDialect.DEFAULT)
                      .withQuoteAllIdentifiers(false)
                      .withAlwaysUseParentheses(false))
          .getSql();
    } catch (RuntimeException e) {
      // An expression that SQL cannot render without its query, like a subquery
      return expression.toString();
    }
  }

  @Override
  public SqlNode toSql(@Nullable RexProgram program, RexNode rex) {
    if (rex instanceof RexDynamicParam param) {
      return new NamedParameter(param.getIndex());
    }

    if (rex instanceof RexCall call && call.isA(SqlKind.CAST)) {
      var type = call.getType();
      var typeSpec =
          new SqlDataTypeSpec(
              new SqlAlienSystemTypeNameSpec(
                  typeName(type), type.getSqlTypeName(), SqlParserPos.ZERO),
              SqlParserPos.ZERO);

      return SqlStdOperatorTable.CAST.createCall(
          SqlParserPos.ZERO, toSql(program, call.getOperands().get(0)), typeSpec);
    }

    return super.toSql(program, rex);
  }

  /** A table function parameter by name, or {@code ?} for a lookup parameter. */
  private class NamedParameter extends SqlDynamicParam {

    NamedParameter(int index) {
      super(index, SqlParserPos.ZERO);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      var index = getIndex();
      writer.literal(index < parameterNames.size() ? ":" + parameterNames.get(index) : "?");
    }
  }
}
