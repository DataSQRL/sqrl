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
package com.datasqrl.engine.database.relational.ddl;

import com.datasqrl.calcite.convert.PostgresSqlNodeToString;
import com.datasqrl.sql.SqlDDLStatement;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

@JsonPropertyOrder({"tableName", "sql", "params"})
@AllArgsConstructor
public class InsertStatement implements SqlDDLStatement {

  String tableName;

  RelDataType tableSchema;

  public String getTableName() {
    return tableName;
  }

  public List<String> getParams() {
    return tableSchema.getFieldList().stream()
        .map(RelDataTypeField::getName)
        .collect(Collectors.toList());
  }

  @Override
  public String getSql() {
    // Create the target table identifier
    var targetTable = new SqlIdentifier(tableName, SqlParserPos.ZERO);

    // Create the list of column names
    List<SqlNode> columnNodes = new ArrayList<>();
    for (RelDataTypeField field : tableSchema.getFieldList()) {
      columnNodes.add(new SqlIdentifier(field.getName(), SqlParserPos.ZERO));
    }
    var columns = new SqlNodeList(columnNodes, SqlParserPos.ZERO);

    // Create the list of values to be inserted
    List<SqlNode> valueNodes = new ArrayList<>();
    for (var i = 0; i < tableSchema.getFieldList().size(); i++) {
      valueNodes.add(new SqlDynamicParam(i, SqlParserPos.ZERO));
    }
    var values =
        new SqlNodeList(
            Collections.singletonList(new SqlNodeList(valueNodes, SqlParserPos.ZERO)),
            SqlParserPos.ZERO);

    // Create the INSERT statement
    var sqlInsert =
        new SqlInsert(SqlParserPos.ZERO, SqlNodeList.EMPTY, targetTable, values, columns);

    // Convert the INSERT statement to a SQL string
    var sql = addValuesKeyword(new PostgresSqlNodeToString().convert(() -> sqlInsert).getSql());
    return sql;
  }

  // workaround as SqlInsert does not support VALUES keyword ??? TODO
  public String addValuesKeyword(String sql) {
    var sb = new StringBuilder(sql);
    var insertPosition = sb.indexOf(")") + 1;
    sb.insert(insertPosition, " VALUES");
    return sb.toString();
  }
}
