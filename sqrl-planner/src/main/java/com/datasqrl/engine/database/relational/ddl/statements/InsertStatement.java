package com.datasqrl.engine.database.relational.ddl.statements;

import com.datasqrl.calcite.convert.PostgresSqlNodeToString;
import com.datasqrl.sql.SqlDDLStatement;
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
    SqlIdentifier targetTable = new SqlIdentifier(tableName, SqlParserPos.ZERO);

    // Create the list of column names
    List<SqlNode> columnNodes = new ArrayList<>();
    for (RelDataTypeField field : tableSchema.getFieldList()) {
      columnNodes.add(new SqlIdentifier(field.getName(), SqlParserPos.ZERO));
    }
    SqlNodeList columns = new SqlNodeList(columnNodes, SqlParserPos.ZERO);

    // Create the list of values to be inserted
    List<SqlNode> valueNodes = new ArrayList<>();
    for (int i = 0; i < tableSchema.getFieldList().size(); i++) {
      valueNodes.add(new SqlDynamicParam(i, SqlParserPos.ZERO));
    }
    SqlNodeList values = new SqlNodeList(Collections.singletonList(new SqlNodeList(valueNodes, SqlParserPos.ZERO)), SqlParserPos.ZERO);

    // Create the INSERT statement
    SqlInsert sqlInsert = new SqlInsert(SqlParserPos.ZERO, SqlNodeList.EMPTY, targetTable, values, columns);

    // Convert the INSERT statement to a SQL string
    String sql = addValuesKeyword(new PostgresSqlNodeToString().convert(()->sqlInsert).getSql());
    return sql;
  }

  // workaround as SqlInsert does not support VALUES keyword ??? TODO
  public String addValuesKeyword(String sql) {
    StringBuilder sb = new StringBuilder(sql);
    int insertPosition = sb.indexOf(")") + 1;
    sb.insert(insertPosition, " VALUES");
    return sb.toString();
  }
}
