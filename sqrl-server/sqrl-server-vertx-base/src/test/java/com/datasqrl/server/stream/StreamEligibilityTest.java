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
import static org.mockito.Mockito.when;

import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.SqlClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class StreamEligibilityTest {

  private StreamEligibility eligibility;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void init() {
    var client = mock(SqlClient.class);
    when(client.preparedQuery(anyString())).thenReturn(mock(PreparedQuery.class));
    eligibility = OrdersApi.eligibility(OrdersApi.engine(client));
  }

  @Test
  void givenScalarColumnsOfListField_whenPlanned_thenStreamsThemInSelectionOrder() {
    var plan = eligibility.plan("{ orders(limit: 5) { paid id tags } }", null);

    assertThat(plan).isPresent();
    assertThat(plan.get().responseKey()).isEqualTo("orders");
    assertThat(plan.get().nonNullField()).isFalse();
    assertThat(plan.get().columns())
        .extracting(StreamedColumn::responseKey)
        .containsExactly("paid", "id", "tags");
  }

  @Test
  void givenAliases_whenPlanned_thenResponseKeysAreAliases() {
    var plan = eligibility.plan("{ latest: orders { orderId: id } }", null);

    assertThat(plan).isPresent();
    assertThat(plan.get().responseKey()).isEqualTo("latest");
    assertThat(plan.get().columns().get(0).responseKey()).isEqualTo("orderId");
    assertThat(plan.get().columns().get(0).columnName()).isEqualTo("id");
  }

  @Test
  void givenFieldMappedToColumn_whenPlanned_thenReadsThatColumn() {
    var plan = eligibility.plan("{ orders { customer } }", null);

    assertThat(plan).isPresent();
    assertThat(plan.get().columns().get(0).columnName()).isEqualTo("customer_name");
  }

  @Test
  void givenColumnTypes_whenPlanned_thenRecordsNullabilityAndListShape() {
    var columns = eligibility.plan("{ orders { id scores tags } }", null).orElseThrow().columns();

    assertThat(columns.get(0))
        .extracting(StreamedColumn::nonNull, StreamedColumn::list)
        .containsExactly(true, false);
    assertThat(columns.get(1))
        .extracting(StreamedColumn::nonNull, StreamedColumn::list, StreamedColumn::itemNonNull)
        .containsExactly(false, true, true);
    assertThat(columns.get(2))
        .extracting(StreamedColumn::list, StreamedColumn::itemNonNull)
        .containsExactly(true, false);
  }

  @Test
  void givenNonNullListField_whenPlanned_thenPlanKnowsTheFieldIsNonNull() {
    var plan = eligibility.plan("{ requiredOrders { id } }", null);

    assertThat(plan).isPresent();
    assertThat(plan.get().nonNullField()).isTrue();
  }

  @Test
  void givenNamedOperationAndMatchingOperationName_whenPlanned_thenStreams() {
    assertThat(eligibility.plan("query Recent($n: Int) { orders(limit: $n) { id } }", "Recent"))
        .isPresent();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{ orders { id items { sku } } }",
        "{ orders { id address { city } } }",
        "{ orders { id status } }",
        "{ orders { id __typename } }",
        "{ orders { ...OrderFields } } fragment OrderFields on Order { id }",
        "{ orders { ... on Order { id } } }",
        "{ orders { id @include(if: true) } }",
        "{ orders @skip(if: false) { id } }",
        "query @cached { orders { id } }",
        "{ orders { id id: paid } }",
        "{ orders { id } requiredOrders { id } }",
        "{ pagedOrders { id } }",
        "{ duckOrders { id } }",
        "{ order(id: 1) { id } }",
        "{ orders { unknown } }",
        "{ unknown { id } }",
        "mutation { orders { id } }",
        "subscription { orders { id } }",
        "query A { orders { id } } query B { orders { id } }",
        "{ orders { id }",
      })
  void givenRequestOutsideTheRules_whenPlanned_thenFallsBack(String query) {
    assertThat(eligibility.plan(query, null)).isEmpty();
  }

  @Test
  void givenOperationNameNotInDocument_whenPlanned_thenFallsBack() {
    assertThat(eligibility.plan("query Recent { orders { id } }", "Other")).isEmpty();
  }
}
