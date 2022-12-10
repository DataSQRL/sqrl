/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util.data;

import com.datasqrl.io.DataSystemDiscoveryConfig;
import com.datasqrl.io.impl.file.DirectoryDataSystemConfig;
import com.datasqrl.util.TestDataset;
import com.datasqrl.util.TestGraphQLSchema;
import com.datasqrl.util.TestScript;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.apache.flink.util.ArrayUtils;

@AllArgsConstructor
public class Nutshop implements TestDataset {

  public enum Variant {
    small, medium, compress, url;
  }

  public static final Path BASE_PATH = Path.of("..", "sqrl-examples", "nutshop");

  public static final Nutshop INSTANCE = new Nutshop(Variant.small);

  public static final Nutshop MEDIUM = new Nutshop(Variant.medium);

  public static final Nutshop COMPRESS = new Nutshop(Variant.compress);

  public static final Nutshop URL = new Nutshop(Variant.url) {
    @Override
    public DataSystemDiscoveryConfig getDiscoveryConfig() {
      return DirectoryDataSystemConfig.Discovery.builder()
          .fileURIs(List.of(
              "https://github.com/DataSQRL/sqrl/raw/55628dff255ffdf4c6de879ea1f2abe4b54d5e99/sqrl-examples/nutshop/data-compress/orders_part1.json.gz",
              "https://github.com/DataSQRL/sqrl/raw/55628dff255ffdf4c6de879ea1f2abe4b54d5e99/sqrl-examples/nutshop/data-compress/products.csv.gz"))
          .build();
    }
  };

  final Variant variant;

  @Override
  public String getName() {
    return "nutshop-" + variant.name();
  }

  @Override
  public Path getDataDirectory() {
    return BASE_PATH.resolve("data-" + variant.name());
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
    if (variant == Variant.small) {
      baseTables = new String[]{"orders", "items", "totals", "customers", "products"};
    } else {
      baseTables = new String[]{"products"};
    }
    return List.of(
        TestScript.of(this,
                BASE_PATH.resolve("customer360").resolve("nutshopv1-" + variant.name() + ".sqrl"),
                ArrayUtils.concat(baseTables, new String[]{"spending_by_month"})).dataSnapshot(false)
            .graphQLSchemas(TestGraphQLSchema.Directory.of(
                BASE_PATH.resolve("customer360").resolve("v1graphql")))
            .build(),
        TestScript.of(this,
                BASE_PATH.resolve("customer360").resolve("nutshopv2-" + variant.name() + ".sqrl"),
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
