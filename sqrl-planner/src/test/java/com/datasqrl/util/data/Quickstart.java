/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util.data;

import com.datasqrl.io.DataSystemDiscoveryConfig;
import com.datasqrl.io.impl.file.DirectoryDataSystemConfig;
import com.datasqrl.util.TestDataset;
import lombok.AllArgsConstructor;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

@AllArgsConstructor
public class Quickstart implements TestDataset {

  public static final Path BASE_PATH = Path.of("..", "sqrl-examples", "quickstart");

  public static final Quickstart INSTANCE = new Quickstart();

  @Override
  public String getName() {
    return "schema";
  }

  @Override
  public Path getDataDirectory() {
    return BASE_PATH.resolve("data");
  }

  @Override
  public Set<String> getTables() {
    return Set.of("products", "orders");
  }

  @Override
  public Path getRootPackageDirectory() {
    return BASE_PATH;
  }

  @Override
  public String toString() {
    return "quickstart";
  }

  @Override
  public DataSystemDiscoveryConfig getDiscoveryConfig() {
    String baseUrl = "https://github.com/DataSQRL/sqrl/raw/651b944d4597865cf020c8fb8b73aca18aa1c3ca/sqrl-examples/quickstart/data/%s";
    return DirectoryDataSystemConfig.Discovery.builder()
            .fileURIs(List.of(
                    String.format(baseUrl,"orders_part1.json.gz"),
                    String.format(baseUrl,"products.csv.gz")))
            .build();
  }

}
