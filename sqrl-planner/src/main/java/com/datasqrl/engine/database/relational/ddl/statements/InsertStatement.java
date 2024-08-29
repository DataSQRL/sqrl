package com.datasqrl.engine.database.relational.ddl.statements;

import com.datasqrl.calcite.dialect.ExtendedPostgresSqlDialect;
import com.datasqrl.sql.SqlDDLStatement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
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

  @Getter
  String tableName;

  RelDataType tableSchema;

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
    String sql = addValuesKeyword(sqlInsert.toSqlString(ExtendedPostgresSqlDialect.DEFAULT).getSql());
    return sql;
  }

  public List<String> getParameters() {
    return tableSchema.getFieldList().stream()
        .map(RelDataTypeField::getName)
        .collect(Collectors.toList());
  }

  // workaround as SqlInsert does not support VALUES keyword ??? TODO
  public String addValuesKeyword(String sql) {
    StringBuilder sb = new StringBuilder(sql);
    int insertPosition = sb.indexOf(")") + 1;
    sb.insert(insertPosition, " VALUES");
    return sb.toString();
  }
}
