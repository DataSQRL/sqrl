/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util.data;

import com.datasqrl.util.ScriptBuilder;
import com.datasqrl.util.TestDataset;
import com.datasqrl.util.TestGraphQLSchema;
import com.datasqrl.util.TestScript;
import java.util.LinkedHashMap;
import lombok.Getter;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

public class Retail implements TestDataset {

  public static final Path BASE_PATH = Path.of("..", "sqrl-examples", "retail");

  public static final Retail INSTANCE = new Retail();

  @Getter
  public final LinkedHashMap<RetailScriptNames, TestScript> testScripts;

  public Retail() {
    testScripts = createTestScripts();
  }

  private LinkedHashMap<RetailScriptNames, TestScript> createTestScripts() {
    LinkedHashMap linkedHashMap = new LinkedHashMap();
    linkedHashMap.put(RetailScriptNames.ORDER_STATS,
        TestScript.of(this, BASE_PATH.resolve("c360-orderstats.sqrl"),
            "orders", "entries", "totals", "customerorderstats").build());

    linkedHashMap.put(RetailScriptNames.FULL,
        TestScript.of(this, BASE_PATH.resolve("c360-full.sqrl"),
                "orders", "entries", "customer", "category", "product", "total", "order_again",
                "_spending_by_month_category", "favorite_categories",
                "order_stats", "newcustomerpromotion")
            .graphQLSchemas(TestGraphQLSchema.Directory.of(BASE_PATH.resolve("c360-full-graphqlv1"),
                BASE_PATH.resolve("c360-full-graphqlv2")))
            .build());
    linkedHashMap.put(RetailScriptNames.RECOMMEND,
        TestScript.of(this, BASE_PATH.resolve("c360-recommend.sqrl"),
            "orders", "entries", "customer", "category", "product", "total",
            "_sales_by_hour", "_sales_24h", "_sales_72h", "_sales_trend").build());
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
    ScriptBuilder builder = new ScriptBuilder();
    builder.append("IMPORT ecommerce-data.Customer");
    builder.append("IMPORT ecommerce-data.Orders");
    builder.append("IMPORT ecommerce-data.Product");
    return builder;
  }

  @Override
  public String toString() {
    return getName();
  }

  public enum RetailScriptNames {
    ORDER_STATS, FULL, RECOMMEND
  }

}
