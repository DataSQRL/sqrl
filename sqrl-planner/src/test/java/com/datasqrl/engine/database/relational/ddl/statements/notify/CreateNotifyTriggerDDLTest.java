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
package com.datasqrl.engine.database.relational.ddl.statements.notify;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

public class CreateNotifyTriggerDDLTest {

  @Test
  public void testGetSql() {
    var ddl = new CreateNotifyTriggerDDL("test_table", Arrays.asList("id", "name"));
    var expectedSql =
        """
        CREATE OR REPLACE FUNCTION notify_on_test_table_insert()
        RETURNS TRIGGER AS $$
        BEGIN
           PERFORM pg_notify('test_table_notify', jsonb_build_object('id', NEW."id", 'name', NEW."name")::text);
           RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER insert_notify_trigger
        AFTER INSERT ON "test_table"
        FOR EACH ROW EXECUTE PROCEDURE notify_on_test_table_insert();\
        """;
    assertEquals(expectedSql, ddl.getSql());
  }

  @Test
  public void testEmptyPrimaryKeys() {
    Exception exception =
        assertThrows(
            IllegalStateException.class,
            () -> {
              new CreateNotifyTriggerDDL("test_table", Collections.emptyList());
            });
    assertEquals(
        "There should be at least one primary key to generate a notify payload.",
        exception.getMessage());
  }
}
