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
package com.datasqrl.container.testing;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.sql.Statement;
import lombok.SneakyThrows;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;

public class ServerFunctionContainerIT extends SqrlWithPostgresContainerTestBase {

  @Override
  protected String getTestCaseName() {
    return "server-functions";
  }

  @Override
  protected void executeStatements(Statement stmt) throws SQLException {
    // Create MyTable and populate with values 1-10
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS \"Customers\" ("
            + "\"customerid\" BIGINT NOT NULL,"
            + "\"email\" TEXT NOT NULL,"
            + "\"name\" TEXT NOT NULL,"
            + "\"lastUpdated\" BIGINT NOT NULL,"
            + "\"timestamp\" TIMESTAMP WITH TIME ZONE NOT NULL,"
            + "PRIMARY KEY (\"customerid\",\"lastUpdated\"))");

    stmt.execute(
        "INSERT INTO \"Customers\" VALUES (1, 'bob.jones@example.com', 'Bob Jones', 1730700002000, '2025-11-11T00:00:00Z')");
  }

  @Test
  @SneakyThrows
  void givenScript_whenCompiledAndServerStarted_thenApiRespondsCorrectly() {
    compileAndStartServerWithDatabase(testDir);
    var response =
        executeGraphQLQuery(
            "{\"query\":\"query { CustomersByName(inputName: \\\"Bobasd\\\") { customerid, name } }\"}");

    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);

    var responseBody = EntityUtils.toString(response.getEntity());
    assertThat(responseBody).contains("Bob Jones");
  }
}
