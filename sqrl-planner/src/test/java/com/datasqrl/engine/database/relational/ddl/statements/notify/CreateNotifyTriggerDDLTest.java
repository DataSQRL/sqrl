package com.datasqrl.engine.database.relational.ddl.statements.notify;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;

public class CreateNotifyTriggerDDLTest {

  @Test
  public void testGetSql() {
    CreateNotifyTriggerDDL ddl = new CreateNotifyTriggerDDL("test_table", Arrays.asList("id", "name"));
    String expectedSql = "CREATE OR REPLACE FUNCTION notify_on_test_table_insert()\n" +
        "RETURNS TRIGGER AS $$\n" +
        "BEGIN\n" +
        "   PERFORM pg_notify('test_table_notify', jsonb_build_object('id', NEW.\"id\", 'name', NEW.\"name\")::text);\n" +
        "   RETURN NEW;\n" +
        "END;\n" +
        "$$ LANGUAGE plpgsql;\n" +
        "\n" +
        "CREATE TRIGGER insert_notify_trigger\n" +
        "AFTER INSERT ON \"test_table\"\n" +
        "FOR EACH ROW EXECUTE PROCEDURE notify_on_test_table_insert();";
    assertEquals(expectedSql, ddl.getSql());
  }

  @Test
  public void testEmptyPrimaryKeys() {
    Exception exception = assertThrows(IllegalStateException.class, () -> {
      new CreateNotifyTriggerDDL("test_table", Collections.emptyList());
    });
    assertEquals("There should be at least one primary key to generate a notify payload.", exception.getMessage());
  }
}