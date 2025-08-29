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
package com.datasqrl.planner.parser;

import static com.datasqrl.planner.parser.SqlScriptStatementSplitter.addStatementDelimiter;
import static com.datasqrl.planner.parser.SqlScriptStatementSplitter.removeStatementDelimiter;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator;
import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.commons.collections4.ListUtils;

/** Represents a column definition that extends a previous table definition */
public class SqrlAddColumnStatement extends SqrlDefinition implements StackableStatement {

  public static final String ALTER_VIEW_PREFIX = "ALTER VIEW %s AS ";
  public static final String ADD_COLUMN_PREFIX = "SELECT %s, ";
  public static final String ADD_COLUMN_SQL_FRAGMENT = "%s AS %s FROM \n( %s )";

  private String addColumnPrefix = ADD_COLUMN_PREFIX.formatted("*");

  public SqrlAddColumnStatement(
      ParsedObject<NamePath> tableName,
      ParsedObject<String> definitionBody,
      SqrlComments comments) {
    super(tableName, definitionBody, AccessModifier.INHERIT, comments);
  }

  public void setColumnNames(List<String> columnNames) {
    var escapedAndFiltered =
        columnNames.stream()
            .filter(col -> !col.equals(getColumnName()))
            .map("`%s`"::formatted)
            .toList();
    this.addColumnPrefix = ADD_COLUMN_PREFIX.formatted(String.join(", ", escapedAndFiltered));
  }

  @Override
  public String toSql(Sqrl2FlinkSQLTranslator sqrlEnv, List<StackableStatement> stack) {
    StatementParserException.checkFatal(
        tableName.get().size() > 1,
        tableName.getFileLocation(),
        ErrorLabel.GENERIC,
        "Column expression requires column name: %s",
        tableName.get());
    var table = this.tableName.get().popLast();

    // It's guaranteed that all other entries in the stack must be add-column statements on the same
    // table
    var innerSql =
        removeStatementDelimiter(((SqrlTableDefinition) stack.get(0)).definitionBody.get());
    for (StackableStatement col : ListUtils.union(stack.subList(1, stack.size()), List.of(this))) {
      Preconditions.checkArgument(col instanceof SqrlAddColumnStatement);
      var column = (SqrlAddColumnStatement) col;
      innerSql = column.addColumn(innerSql);
    }
    var sql =
        String.format(ALTER_VIEW_PREFIX + "%s", table.toString(), addStatementDelimiter(innerSql));
    return sql;
  }

  @Override
  String getPrefix() {
    return String.format(ALTER_VIEW_PREFIX + addColumnPrefix, this.tableName.get().popLast());
  }

  @Override
  public FileLocation mapSqlLocation(FileLocation location) {
    return definitionBody
        .getFileLocation()
        .add(SQLStatement.removeFirstRowOffset(location, getPrefix().length()));
  }

  private String getColumnName() {
    return tableName.get().getLast().getDisplay();
  }

  private String addColumn(String innerBody) {
    return addColumnPrefix
        + ADD_COLUMN_SQL_FRAGMENT.formatted(
            removeStatementDelimiter(definitionBody.get()), getColumnName(), innerBody);
  }

  @Override
  public boolean isRoot() {
    return false;
  }
}
