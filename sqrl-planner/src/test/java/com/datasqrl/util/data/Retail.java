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
package com.datasqrl.util.data;

import com.datasqrl.util.ScriptBuilder;
import com.datasqrl.util.TestDataset;
import com.datasqrl.util.TestGraphQLSchema;
import com.datasqrl.util.TestScript;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Set;
import lombok.Getter;

public class Retail implements TestDataset {

  public static final Path BASE_PATH = Path.of("..", "..", "sqrl-examples", "retail");

  public static final Retail INSTANCE = new Retail();

  @Getter public final LinkedHashMap<RetailScriptNames, TestScript> testScripts;

  public Retail() {
    testScripts = createTestScripts();
  }

  private LinkedHashMap<RetailScriptNames, TestScript> createTestScripts() {
    var linkedHashMap = new LinkedHashMap();
    linkedHashMap.put(
        RetailScriptNames.ORDER_STATS,
        TestScript.of(
                this,
                BASE_PATH.resolve("c360-orderstats.sqrl"),
                "orders",
                "entries",
                "totals",
                "customerorderstats")
            .build());

    linkedHashMap.put(
        RetailScriptNames.FULL,
        TestScript.of(
                this,
                BASE_PATH.resolve("c360-full.sqrl"),
                "orders",
                "entries",
                "customer",
                "category",
                "product",
                "total",
                "order_again",
                "_spending_by_month_category",
                "favorite_categories",
                "order_stats",
                "newcustomerpromotion")
            .graphQLSchemas(
                TestGraphQLSchema.Directory.of(
                    BASE_PATH.resolve("c360-full-graphqlv1"),
                    BASE_PATH.resolve("c360-full-graphqlv2")))
            .build());
    linkedHashMap.put(
        RetailScriptNames.RECOMMEND,
        TestScript.of(
                this,
                BASE_PATH.resolve("c360-recommend.sqrl"),
                "orders",
                "entries",
                "customer",
                "category",
                "product",
                "total",
                "_sales_by_hour",
                "_sales_24h",
                "_sales_72h",
                "_sales_trend")
            .build());
    linkedHashMap.put(
        RetailScriptNames.SEARCH,
        TestScript.of(this, BASE_PATH.resolve("c360-search.sqrl"), "product")
            .graphQLSchemas(
                TestGraphQLSchema.Directory.of(BASE_PATH.resolve("c360-search-graphql")))
            .build());
    linkedHashMap.put(
        RetailScriptNames.AVRO_KAFKA,
        TestScript.of(this, BASE_PATH.resolve("c360-kafka.sqrl"), "ordercount")
            .graphQLSchemas(TestGraphQLSchema.Directory.of(BASE_PATH.resolve("c360-kafka-graphql")))
            .build());
    return linkedHashMap;
  }

  @Override
  public String getName() {
    return "ecommerce-data";
  }

  @Override
  public Path getDataDirectory() {
    return BASE_PATH.resolve("data");
  }

  @Override
  public Set<String> getTables() {
    return Set.of("customer", "product", "orders");
  }

  @Override
  public Path getRootPackageDirectory() {
    return BASE_PATH;
  }

  public TestScript getScript(RetailScriptNames name) {
    return testScripts.get(name);
  }

  public ScriptBuilder getImports() {
    var builder = new ScriptBuilder();
    builder.append("IMPORT time.*");
    builder.append("IMPORT ecommerce-data.Customer TIMESTAMP _ingest_time");
    builder.append("IMPORT ecommerce-data.Orders TIMESTAMP time");
    builder.append("IMPORT ecommerce-data.Product TIMESTAMP _ingest_time");
    return builder;
  }

  @Override
  public String toString() {
    return getName();
  }

  public enum RetailScriptNames {
    ORDER_STATS,
    FULL,
    RECOMMEND,
    SEARCH,
    AVRO_KAFKA
  }
}
