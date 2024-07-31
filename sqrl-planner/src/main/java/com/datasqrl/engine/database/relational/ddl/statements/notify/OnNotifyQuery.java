package com.datasqrl.engine.database.relational.ddl.statements.notify;

import static com.datasqrl.engine.database.relational.ddl.PostgresDDLFactory.quoteIdentifier;

import com.datasqrl.sql.SqlDDLStatement;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class OnNotifyQuery implements SqlDDLStatement {

  String tableName;
  List<Parameter> parameters;

  @Override
  public String getSql() {
    return String.format("SELECT * FROM %s WHERE %s;", quoteIdentifier(tableName), getArgumentTemplate());
  }

  private String getArgumentTemplate() {
    return parameters.stream()
        .map(p -> String.format("%s = ?::%s", quoteIdentifier(p.getName()), p.getType()))
        .collect(Collectors.joining(", "));
  }
}
