/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util.data;

import com.datasqrl.util.TestDataset;
import com.datasqrl.util.TestGraphQLSchema;
import com.datasqrl.util.TestScript;
import lombok.AllArgsConstructor;
import org.apache.flink.util.ArrayUtils;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

@AllArgsConstructor
public class Nutshop implements TestDataset {

  public enum Size {
    small, medium;
  }

  public static final Path BASE_PATH = Path.of("..", "sqrl-examples", "nutshop");

  public static final Nutshop INSTANCE = new Nutshop(Size.small);

  public static final Nutshop MEDIUM = new Nutshop(Size.medium);

  final Size size;

  @Override
  public String getName() {
    return "nutshop-" + size.name();
  }

  @Override
  public Path getDataDirectory() {
    return BASE_PATH.resolve("data-" + size.name());
  }

  @Override
  public Set<String> getTables() {
    return Set.of("products", "orders");
  }

  @Override
  public Path getRootPackageDirectory() {
    return BASE_PATH;
  }

  public List<TestScript> getScripts() {
    String[] baseTables;
    if (size == Size.small) {
      baseTables = new String[]{"orders", "items", "totals", "customers", "products"};
    } else {
      baseTables = new String[]{"products"};
    }
    return List.of(
        TestScript.of(this,
                BASE_PATH.resolve("customer360").resolve("nutshopv1-" + size.name() + ".sqrl"),
                ArrayUtils.concat(baseTables, new String[]{"spending_by_month"})).dataSnapshot(false)
            .build(),
        TestScript.of(this,
                BASE_PATH.resolve("customer360").resolve("nutshopv2-" + size.name() + ".sqrl"),
                ArrayUtils.concat(baseTables, new String[]{"spending_by_month",
                    "past_purchases", "volume_by_day"})).dataSnapshot(false)
            .graphQLSchemas(TestGraphQLSchema.Directory.of(
                BASE_PATH.resolve("customer360").resolve("v2graphql")))
            .build()
    );
  }

  @Override
  public String toString() {
    return getName();
  }


}
