package com.datasqrl.engine.database.relational.ddl.statements.notify;

import java.util.List;
import java.util.stream.Collectors;

import com.datasqrl.sql.SqlDDLStatement;
import com.google.common.base.Preconditions;

import lombok.NonNull;

public class CreateNotifyTriggerDDL implements SqlDDLStatement {

  String tableName;
  List<String> primaryKeys;

  public CreateNotifyTriggerDDL(@NonNull String tableName, @NonNull List<String> primaryKeys) {
    this.tableName = tableName;
    Preconditions.checkState(!primaryKeys.isEmpty(), "There should be at least one primary key to generate a notify payload.");
    this.primaryKeys = primaryKeys;
  }

  @Override
  public String getSql() {
    return String.format(
        """
			CREATE OR REPLACE FUNCTION notify_on_%1$s_insert()
			RETURNS TRIGGER AS $$
			BEGIN
			   PERFORM pg_notify('%1$s_notify', %2$s);
			   RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;

			CREATE TRIGGER insert_notify_trigger
			AFTER INSERT ON "%1$s"
			FOR EACH ROW EXECUTE PROCEDURE notify_on_%1$s_insert();""", tableName, createPayload());
  }

  private String createPayload() {
    var argumentList = primaryKeys.stream()
        .map(pk ->
            String.format("'%s', NEW.\"%s\"", pk, pk))
        .collect(Collectors.joining(", "));

    return String.format("jsonb_build_object(%s)::text", argumentList);
  }
}
