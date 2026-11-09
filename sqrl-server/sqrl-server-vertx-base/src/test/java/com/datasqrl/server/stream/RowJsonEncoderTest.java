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

import static com.datasqrl.server.stream.OrdersApi.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import graphql.ExecutionInput;
import graphql.GraphQL;
import graphql.GraphQLContext;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.data.Numeric;
import io.vertx.sqlclient.impl.RowSetImpl;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RowJsonEncoderTest {

  private static final String ALL_COLUMNS_QUERY =
      """
      { orders {
          id customer amount total quantity small paid createdAt localTs day time
          payload tags scores uuid blob renamed: mixedCase
      } }""";

  private static final List<String> COLUMNS =
      List.of(
          "id",
          "customer_name",
          "amount",
          "total",
          "quantity",
          "small",
          "paid",
          "createdat",
          "localts",
          "day",
          "time",
          "payload",
          "tags",
          "scores",
          "uuid",
          "blob",
          "MIXEDCASE");

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
  void givenRowsOfEveryColumnType_whenStreamed_thenBytesEqualNonStreamingResponse() {
    var rows = OrdersApi.rows(COLUMNS, everyColumnType());

    assertThat(streamed(ALL_COLUMNS_QUERY, rows, false))
        .isEqualTo(nonStreamed(ALL_COLUMNS_QUERY, rows));
  }

  @Test
  void givenOutputDrainedAfterEveryRow_whenStreamed_thenChunksConcatenateToNonStreamingResponse() {
    var rows = OrdersApi.rows(COLUMNS, everyColumnType());

    assertThat(streamed(ALL_COLUMNS_QUERY, rows, true))
        .isEqualTo(nonStreamed(ALL_COLUMNS_QUERY, rows));
  }

  @Test
  void givenNoRows_whenStreamed_thenBytesEqualNonStreamingEmptyList() {
    var rows = OrdersApi.rows(COLUMNS, List.of());

    assertThat(streamed(ALL_COLUMNS_QUERY, rows, false))
        .isEqualTo(nonStreamed(ALL_COLUMNS_QUERY, rows))
        .isEqualTo("{\"data\":{\"orders\":[]}}");
  }

  @Test
  void givenNullableFieldQueryFailsBeforeFirstByte_whenErrorResponseBuilt_thenEqualsNonStreaming() {
    assertErrorResponseMatchesNonStreaming("{ orders { id } }");
  }

  @Test
  void givenNonNullFieldQueryFailsBeforeFirstByte_whenErrorResponseBuilt_thenEqualsNonStreaming() {
    assertErrorResponseMatchesNonStreaming("{ requiredOrders { id } }");
  }

  @Test
  void givenRowsAlreadyWritten_whenFailed_thenClosesRowsAndAppendsError() {
    var encoder = encoder("{ orders { id } }");
    OrdersApi.rows(List.of("id"), List.of(row(1), row(2))).forEach(encoder::writeRow);

    encoder.fail(new IllegalStateException("cursor lost"));

    assertThat(encoder.drain().toString())
        .isEqualTo(
            "{\"data\":{\"orders\":[{\"id\":1},{\"id\":2}]},\"errors\":[{\"message\":\"Exception"
                + " while fetching data (/orders) : cursor lost\",\"locations\":[{\"line\":1,"
                + "\"column\":3}],\"path\":[\"orders\"],\"extensions\":{\"classification\":"
                + "\"DataFetchingException\"}}]}");
  }

  @Test
  void givenNullInNonNullColumn_whenRowWritten_thenThrowsWithoutWritingPartialRow() {
    var encoder = encoder("{ orders { customer id } }");
    var rows =
        OrdersApi.rows(List.of("customer_name", "id"), List.of(row("Ann", 1), row("Bob", null)));
    encoder.writeRow(rows.get(0));

    assertThatThrownBy(() -> encoder.writeRow(rows.get(1)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Non-null field 'id' was null");
    encoder.fail(new IllegalStateException("Non-null field 'id' was null"));
    var response = new JsonObject(encoder.drain());
    assertThat(response.getJsonObject("data").getJsonArray("orders"))
        .isEqualTo(new JsonArray().add(new JsonObject().put("customer", "Ann").put("id", 1)));
    assertThat(response.getJsonArray("errors")).hasSize(1);
  }

  @Test
  void givenRowsWritten_whenBuffered_thenBufferedBytesCountsUndrainedOutput() {
    var encoder = encoder("{ orders { id } }");
    encoder.drain();
    OrdersApi.rows(List.of("id"), List.of(row(12345))).forEach(encoder::writeRow);

    assertThat(encoder.bufferedBytes()).isEqualTo("{\"id\":12345}".length());
    assertThat(encoder.drain().length()).isEqualTo("{\"id\":12345}".length());
    assertThat(encoder.bufferedBytes()).isZero();
  }

  private void assertErrorResponseMatchesNonStreaming(String query) {
    var failure = new IllegalStateException("connection refused");
    when(preparedQuery.execute(any(Tuple.class))).thenReturn(Future.failedFuture(failure));
    var nonStreamed = new JsonObject(engine.execute(query).toSpecification()).toBuffer().toString();

    var plan = OrdersApi.eligibility(engine).plan(query, null).orElseThrow();

    assertThat(plan.errorResponse(failure).toString()).isEqualTo(nonStreamed);
  }

  private String nonStreamed(String query, List<Row> rows) {
    RowSet<Row> rowSet = rows.stream().collect(RowSetImpl.COLLECTOR);
    when(preparedQuery.execute(any(Tuple.class))).thenReturn(Future.succeededFuture(rowSet));
    var result = engine.execute(ExecutionInput.newExecutionInput().query(query).build());
    assertThat(result.getErrors()).isEmpty();
    return new JsonObject(result.toSpecification()).toBuffer().toString();
  }

  private String streamed(String query, List<Row> rows, boolean drainEveryRow) {
    var encoder = encoder(query);
    var output = Buffer.buffer();
    for (var row : rows) {
      encoder.writeRow(row);
      if (drainEveryRow) {
        output.appendBuffer(encoder.drain());
      }
    }
    encoder.finish();
    return output.appendBuffer(encoder.drain()).toString();
  }

  private RowJsonEncoder encoder(String query) {
    var plan = OrdersApi.eligibility(engine).plan(query, null).orElseThrow();
    return new RowJsonEncoder(plan, GraphQLContext.getDefault(), Locale.getDefault());
  }

  private static List<List<Object>> everyColumnType() {
    return List.of(
        row(
            1,
            "Ann \"Quote\" é\n\t ",
            12.345678912,
            Numeric.create(new BigDecimal("1234.5600")),
            9_000_000_000L,
            (short) 7,
            true,
            OffsetDateTime.of(2024, 1, 2, 3, 4, 5, 123_456_000, ZoneOffset.ofHours(2)),
            LocalDateTime.of(2024, 1, 2, 3, 4, 5),
            LocalDate.of(2024, 1, 2),
            LocalTime.of(3, 4, 5, 600_000_000),
            new JsonObject().put("a", 1).put("b", new JsonArray().add("x").addNull()),
            new String[] {"a", null, "c"},
            new Integer[] {1, 2},
            UUID.fromString("123e4567-e89b-12d3-a456-426614174000"),
            Buffer.buffer("hi"),
            "upper"),
        row(
            2, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
            null, null),
        row(
            3,
            "",
            1e-10,
            Numeric.create(42),
            -1L,
            (short) -3,
            false,
            OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC),
            LocalDateTime.of(2024, 12, 31, 23, 59, 59, 999_999_999),
            LocalDate.of(1, 1, 1),
            LocalTime.MIDNIGHT,
            new JsonArray().add(1.5).add(true),
            new String[] {},
            new Integer[] {Integer.MAX_VALUE},
            UUID.fromString("00000000-0000-0000-0000-000000000000"),
            Buffer.buffer(new byte[] {0, -1}),
            null),
        row(
            4,
            "x",
            1e20,
            Numeric.create(new BigDecimal("1E+3")),
            0L,
            (short) 0,
            true,
            null,
            null,
            null,
            null,
            new JsonObject(),
            null,
            null,
            null,
            null,
            ""));
  }
}
