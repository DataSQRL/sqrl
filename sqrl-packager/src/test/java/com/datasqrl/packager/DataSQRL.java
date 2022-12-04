/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager;

import com.datasqrl.util.TestDataset;
import com.datasqrl.util.TestGraphQLSchema;
import com.datasqrl.util.TestScript;

import java.nio.file.Path;
import java.util.Set;

public class DataSQRL implements TestDataset {

  public static final Path BASE_PATH = Path.of("..", "sqrl-examples", "datasqrl");

  public static final DataSQRL INSTANCE = new DataSQRL();


  @Override
  public String getName() {
    return "datasqrl-central";
  }

  @Override
  public Path getDataDirectory() {
    return BASE_PATH.resolve("data");
  }

  @Override
  public Path getRootPackageDirectory() {
    return BASE_PATH;
  }

  @Override
  public String getFilePartPattern() {
    return "\\.([-_A-Za-z0-9]+)";
  }

  @Override
  public Set<String> getTables() {
    return Set.of("package");
  }

  public TestScript getScript() {
    return TestScript.of(this, BASE_PATH.resolve("repo.sqrl"),
        "packages", "submissions").build();
  }

  public TestGraphQLSchema getGraphQL() {
    return new TestGraphQLSchema.Directory(BASE_PATH);
  }
}
