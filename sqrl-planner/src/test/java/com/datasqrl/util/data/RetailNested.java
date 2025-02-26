/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util.data;

import com.datasqrl.util.TestDataset;
import com.datasqrl.util.TestScript;
import java.nio.file.Path;
import java.util.Set;
import lombok.Getter;

public class RetailNested implements TestDataset {

  public static final Path BASE_PATH = Path.of("..", "..", "sqrl-examples", "retail-nested");

  public static final RetailNested INSTANCE = new RetailNested();

  @Getter public final TestScript testScript;

  public RetailNested() {
    testScript =
        TestScript.of(this, BASE_PATH.resolve("nested.sqrl"), "orders", "productcount").build();
  }

  @Override
  public String getName() {
    return "nested-data";
  }

  @Override
  public Path getDataDirectory() {
    return BASE_PATH.resolve("data");
  }

  @Override
  public Set<String> getTables() {
    return Set.of("orders");
  }

  @Override
  public Path getRootPackageDirectory() {
    return BASE_PATH;
  }

  @Override
  public String toString() {
    return getName();
  }
}
