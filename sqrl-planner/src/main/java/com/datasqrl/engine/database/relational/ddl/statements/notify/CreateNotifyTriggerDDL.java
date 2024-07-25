package com.datasqrl.engine.database.relational.ddl.statements.notify;

import com.datasqrl.sql.SqlDDLStatement;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class CreateNotifyTriggerDDL implements SqlDDLStatement {

  String tableName;
  List<String> primaryKeys;

  @Override
  public String getSql() {
    return String.format(
        "CREATE OR REPLACE FUNCTION notify_on_%1$s_insert()\n" +
            "RETURNS TRIGGER AS $$\n" +
            "BEGIN\n" +
            "   PERFORM pg_notify('%1$s_notify', %2$s);\n" +
            "   RETURN NEW;\n" +
            "END;\n" +
            "$$ LANGUAGE plpgsql;\n" +
            "\n" +
            "CREATE TRIGGER insert_notify_trigger\n" +
            "AFTER INSERT ON \"%1$s\"\n" +
            "FOR EACH ROW EXECUTE PROCEDURE notify_on_%1$s_insert();", tableName, createPayload());
  }

  private String createPayload() {
    if (primaryKeys.isEmpty()) {
      throw new IllegalArgumentException("There should be at least one primary key to generate a notify payload.");
    }

    return primaryKeys.stream()
        .map(pk -> "NEW." + pk + "::text")
        .collect(Collectors.joining(" || ',' || "));
  }
}
