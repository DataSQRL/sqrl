package com.datasqrl.engine.database.relational.ddl.statements.notify;

import com.datasqrl.sql.SqlDDLStatement;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class OnNotifyQuery implements SqlDDLStatement {

  String tableName;
  List<String> primaryKeys;

  @Override
  public String getSql() {
    return String.format("SELECT * FROM %s WHERE %s;", tableName, getArgumentTemplate());
  }

  private String getArgumentTemplate() {
    return primaryKeys.stream()
        .map(pk -> pk + "= ?")
        .collect(Collectors.joining(", "));
  }
}
