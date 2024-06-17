package com.datasqrl.engine.database.relational.ddl.statements;

import com.datasqrl.sql.SqlDDLStatement;
import lombok.Value;

@Value
public class CreateNotifyTriggerDDL implements SqlDDLStatement {

  String tableName;
  String primaryKey;

  @Override
  public String getSql() {
    return String.format(
        "CREATE OR REPLACE FUNCTION notify_on_%1$s_insert()\n" +
            "RETURNS TRIGGER AS $$\n" +
            "BEGIN\n" +
            "   PERFORM pg_notify('%1$s_notify', NEW.%2$s::text);\n" +
            "   RETURN NEW;\n" +
            "END;\n" +
            "$$ LANGUAGE plpgsql;\n" +
            "\n" +
            "CREATE TRIGGER insert_notify_trigger\n" +
            "AFTER INSERT ON %1$s\n" +
            "FOR EACH ROW EXECUTE PROCEDURE notify_on_%1$s_insert();", tableName, primaryKey);
  }
}
