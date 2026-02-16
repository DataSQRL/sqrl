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
package com.datasqrl.engine.database.relational.ddl.notify;

import com.datasqrl.sql.SqlDDLStatement;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.stream.Collectors;
import lombok.NonNull;

public class CreateNotifyTriggerDDL implements SqlDDLStatement {

  String tableName;
  List<String> primaryKeys;

  public CreateNotifyTriggerDDL(@NonNull String tableName, @NonNull List<String> primaryKeys) {
    this.tableName = tableName;
    Preconditions.checkState(
        !primaryKeys.isEmpty(),
        "There should be at least one primary key to generate a notify payload.");
    this.primaryKeys = primaryKeys;
  }

  @Override
  public String getSql() {
    return """
        CREATE OR REPLACE FUNCTION notify_on_%1$s_insert()
        RETURNS TRIGGER AS $$
        BEGIN
           PERFORM pg_notify('%1$s_notify', %2$s);
           RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER insert_notify_trigger
        AFTER INSERT ON "%1$s"
        FOR EACH ROW EXECUTE PROCEDURE notify_on_%1$s_insert();\
        """
        .formatted(tableName, createPayload());
  }

  private String createPayload() {
    var argumentList =
        primaryKeys.stream()
            .map(pk -> "'%s', NEW.\"%s\"".formatted(pk, pk))
            .collect(Collectors.joining(", "));

    return "jsonb_build_object(%s)::text".formatted(argumentList);
  }
}
