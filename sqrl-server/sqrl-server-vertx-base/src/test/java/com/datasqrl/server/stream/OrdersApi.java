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

import com.datasqrl.server.PaginationType;
import com.datasqrl.server.VertxServerContext;
import com.datasqrl.server.graphql.CustomScalars;
import com.datasqrl.server.graphql.GraphQLEngineBuilder;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.graphql.RootGraphQLModel.ArgumentLookupQueryCoords;
import com.datasqrl.server.graphql.RootGraphQLModel.FieldLookupQueryCoords;
import com.datasqrl.server.graphql.RootGraphQLModel.QueryWithArguments;
import com.datasqrl.server.graphql.RootGraphQLModel.SqlQuery;
import com.datasqrl.server.graphql.RootGraphQLModel.StringSchema;
import com.datasqrl.server.jdbc.DatabaseType;
import com.datasqrl.server.jdbc.VertxJdbcClient;
import com.datasqrl.server.jdbc.VertxParamArgumentTypeMapper;
import graphql.GraphQL;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import io.vertx.sqlclient.impl.RowBase;
import io.vertx.sqlclient.internal.RowDescriptorBase;
import java.sql.JDBCType;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** An orders API whose fields cover every streaming eligibility rule. */
final class OrdersApi {

  static final String ORDERS_SQL = "SELECT * FROM orders ORDER BY id";

  static final String SCHEMA =
      """
      scalar DateTime
      scalar Date
      scalar LocalTime
      scalar JSON
      scalar Long
      type Query {
        orders(limit: Int, offset: Int = 0): [Order!]
        requiredOrders: [Order!]!
        pagedOrders(limit: Int = 10, offset: Int = 0): [Order!]
        duckOrders: [Order!]
        order(id: Int!): Order
      }
      type Mutation {
        orders: [Order!]
      }
      type Order {
        id: Int!
        customer: String
        amount: Float
        total: Float
        quantity: Long
        small: Int
        paid: Boolean
        createdAt: DateTime
        localTs: DateTime
        day: Date
        time: LocalTime
        payload: JSON
        tags: [String]
        scores: [Int!]
        uuid: String
        blob: String
        mixedCase: String
        status: Status
        address: Address
        items(limit: Int): [Item!]
      }
      type Address {
        city: String
      }
      type Item {
        sku: String
      }
      enum Status {
        OPEN
        CLOSED
      }
      """;

  private OrdersApi() {}

  static RootGraphQLModel model() {
    return RootGraphQLModel.builder()
        .schema(StringSchema.builder().schema(SCHEMA).build())
        .query(lookup("Query", "orders", PaginationType.LIMIT_AND_OFFSET, DatabaseType.POSTGRES))
        .query(lookup("Query", "requiredOrders", PaginationType.NONE, DatabaseType.POSTGRES))
        .query(
            lookup("Query", "pagedOrders", PaginationType.OFFSET_PAGE_INFO, DatabaseType.POSTGRES))
        .query(lookup("Query", "duckOrders", PaginationType.NONE, DatabaseType.DUCKDB))
        .query(lookup("Query", "order", PaginationType.NONE, DatabaseType.POSTGRES))
        .query(lookup("Order", "items", PaginationType.LIMIT_AND_OFFSET, DatabaseType.POSTGRES))
        .query(
            FieldLookupQueryCoords.builder()
                .parentType("Order")
                .fieldName("customer")
                .columnName("customer_name")
                .build())
        .build();
  }

  static GraphQL engine(SqlClient client) {
    return model()
        .accept(
            new GraphQLEngineBuilder.Builder()
                .withExtendedScalarTypes(CustomScalars.getExtendedScalars())
                .build(),
            new VertxServerContext(
                new VertxJdbcClient(
                    Map.of(DatabaseType.POSTGRES, client, DatabaseType.DUCKDB, client)),
                Map.of(),
                null,
                new VertxParamArgumentTypeMapper()))
        .build();
  }

  static StreamEligibility eligibility(GraphQL engine) {
    return new StreamEligibility(engine.getGraphQLSchema(), model().getQueries());
  }

  static List<Row> rows(List<String> columns, List<List<Object>> values) {
    var descriptor =
        new RowDescriptorBase(
            columns.stream().map(OrdersApi::column).toArray(ColumnDescriptor[]::new));
    return values.stream().map(row -> (Row) new RowBase(descriptor, row)).toList();
  }

  static List<Object> row(Object... values) {
    return Arrays.asList(values);
  }

  private static ArgumentLookupQueryCoords lookup(
      String parentType, String fieldName, PaginationType pagination, DatabaseType database) {
    return ArgumentLookupQueryCoords.builder()
        .parentType(parentType)
        .fieldName(fieldName)
        .exec(
            QueryWithArguments.builder()
                .query(
                    new SqlQuery(ORDERS_SQL, List.of(), pagination, 0, database, null, null, null))
                .build())
        .build();
  }

  private static ColumnDescriptor column(String name) {
    return new ColumnDescriptor() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public boolean isArray() {
        return false;
      }

      @Override
      public String typeName() {
        return "unknown";
      }

      @Override
      public JDBCType jdbcType() {
        return JDBCType.OTHER;
      }
    };
  }
}
