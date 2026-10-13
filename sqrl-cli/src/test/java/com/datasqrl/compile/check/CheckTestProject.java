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
package com.datasqrl.compile.check;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A small project for checker and language server tests: a main script in {@code project/} that
 * imports a templated script with a data file from an included package in {@code shared/}. The
 * variant with an API also has a relationship, a GraphQL schema and an operation.
 */
public record CheckTestProject(
    Path projectRoot, Path mainScript, Path sharedScript, Path schema, Path operations) {

  public static final String MAIN_SCRIPT =
      """
      IMPORT shop.sources;

      OrderStream := SELECT * FROM sources.Orders;
      Orders := DISTINCT OrderStream ON id ORDER BY updated_at DESC;
      CustomerTotals := SELECT customer_id, SUM(amount) AS total FROM Orders GROUP BY customer_id;
      """;

  public static final String SHARED_SCRIPT =
      """
      CREATE TABLE Orders (
        PRIMARY KEY (id, `updated_at`) NOT ENFORCED,
        WATERMARK FOR `updated_at` AS `updated_at` - INTERVAL '0.001' SECOND
      ) WITH (
        'source.monitor-interval' = '{{interval}}'
      )
      LIKE `orders.jsonl`;
      """;

  public static final String API_SCRIPT =
      MAIN_SCRIPT
          + "Orders.totals := SELECT * FROM CustomerTotals t WHERE t.customer_id = this.customer_id;\n";

  public static final String SCHEMA =
      """
      "An order"
      type Orders {
        id: Long!
        customer_id: Long!
        amount: Float!
        totals(limit: Int = 10, offset: Int = 0): [CustomerTotals!]
      }

      type CustomerTotals {
        customer_id: Long!
        total: Float!
      }

      "A 64-bit signed integer"
      scalar Long

      type Query {
        "The latest version of each order"
        Orders(limit: Int = 10, offset: Int = 0): [Orders!]
        CustomerTotals(limit: Int = 10, offset: Int = 0): [CustomerTotals!]
      }
      """;

  public static final String OPERATIONS =
      """
      query BigOrders {
        orders: Orders(limit: 5) {
          id
          totals {
            total
          }
        }
      }
      """;

  public static CheckTestProject create(Path dir) throws IOException {
    return create(dir, false);
  }

  /** Creates the project with a GraphQL schema and operation that the API is checked against. */
  public static CheckTestProject createWithApi(Path dir) throws IOException {
    return create(dir, true);
  }

  private static CheckTestProject create(Path dir, boolean api) throws IOException {
    var shared = Files.createDirectories(dir.resolve("shared"));
    Files.writeString(
        shared.resolve("package.json"),
        """
        {"version": "1", "script": {"config": {"interval": "10 sec"}}}
        """);
    Files.writeString(
        shared.resolve("orders.jsonl"),
        """
        {"id": 1, "customer_id": 10, "amount": 12.5, "updated_at": "2024-01-01T10:00:00.000Z"}
        {"id": 2, "customer_id": 11, "amount": 7.0, "updated_at": "2024-01-01T11:00:00.000Z"}
        """);
    var sharedScript = shared.resolve("sources.sqrl");
    Files.writeString(sharedScript, SHARED_SCRIPT);

    var project = Files.createDirectories(dir.resolve("project"));
    Files.writeString(
        project.resolve("package.json"),
        """
        {
          "version": "1",
          "enabled-engines": ["vertx", "postgres", "kafka", "flink"],
          "script": {
            "main": "main.sqrl",%s
            "include": {"shop": {"package": "../shared/package.json"}}
          }
        }
        """
            .formatted(
                api
                    ? """

                      "graphql": "schema.graphqls",
                      "operations": ["queries.graphql"],"""
                    : ""));
    var mainScript = project.resolve("main.sqrl");
    Files.writeString(mainScript, api ? API_SCRIPT : MAIN_SCRIPT);
    var schema = project.resolve("schema.graphqls");
    var operations = project.resolve("queries.graphql");
    if (api) {
      Files.writeString(schema, SCHEMA);
      Files.writeString(operations, OPERATIONS);
    }
    return new CheckTestProject(
        project.toAbsolutePath().normalize(),
        mainScript.toAbsolutePath().normalize(),
        sharedScript.toAbsolutePath().normalize(),
        schema.toAbsolutePath().normalize(),
        operations.toAbsolutePath().normalize());
  }
}
