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
package com.datasqrl.server.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import graphql.ExecutionInput;
import graphql.GraphQL;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PendingStreamTest {

  private PreparedQuery<RowSet<Row>> preparedQuery;
  private GraphQL engine;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void init() {
    var client = mock(SqlClient.class);
    preparedQuery = mock(PreparedQuery.class);
    when(client.preparedQuery(anyString())).thenReturn(preparedQuery);
    engine = OrdersApi.engine(client);
  }

  @Test
  void givenPendingStreamInContext_whenExecuted_thenBindsTheStatementInsteadOfRunningIt() {
    var pending = new PendingStream();

    var result =
        engine.execute(
            ExecutionInput.newExecutionInput()
                .query("query Recent($n: Int) { orders(limit: $n, offset: 2) { id } }")
                .variables(Map.of("n", 5))
                .graphQLContext(Map.of(PendingStream.class, pending))
                .build());

    assertThat(result.getErrors()).isEmpty();
    assertThat(result.<Map<String, Object>>getData()).containsEntry("orders", List.of());
    assertThat(pending.isBound()).isTrue();
    assertThat(pending.getSql())
        .isEqualTo("SELECT * FROM (SELECT * FROM orders ORDER BY id) x LIMIT $1 OFFSET $2");
    assertThat(pending.getParams().getInteger(0)).isEqualTo(5);
    assertThat(pending.getParams().getInteger(1)).isEqualTo(2);
    verifyNoInteractions(preparedQuery);
  }

  @Test
  void givenInvalidArgument_whenExecutedWithPendingStream_thenValidationFailsAndNothingIsBound() {
    var pending = new PendingStream();

    var result =
        engine.execute(
            ExecutionInput.newExecutionInput()
                .query("{ orders(limit: \"five\") { id } }")
                .graphQLContext(Map.of(PendingStream.class, pending))
                .build());

    assertThat(result.getErrors()).hasSize(1);
    assertThat(pending.isBound()).isFalse();
    verifyNoInteractions(preparedQuery);
  }
}
