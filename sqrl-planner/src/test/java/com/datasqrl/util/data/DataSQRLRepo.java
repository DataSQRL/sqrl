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

public class DataSQRLRepo implements TestDataset {

  public static final Path BASE_PATH = Path.of("..", "sqrl-examples", "repository");

  public static final DataSQRLRepo INSTANCE = new DataSQRLRepo();

  @Override
  public String getName() {
    return "repository";
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
  public DataSystemDiscoveryConfig getDiscoveryConfig() {
    return DirectoryDataSystemConfig.Discovery.builder()
            .directoryURI(getDataDirectory().toUri().getPath())
            .filenamePattern("([^\\.]+?)\\.(?:[-_A-Za-z0-9]+)")
            .build();
  }

  @Override
  public Set<String> getTables() {
    return Set.of("package");
  }

  public TestScript getScript() {
    return TestScript.of(this, BASE_PATH.resolve("repo.sqrl"),
        "package", "submission").graphQLSchemas(List.of(getGraphQL())).build();
  }

  public TestGraphQLSchema getGraphQL() {
    return new TestGraphQLSchema.Directory(BASE_PATH.resolve("graphql"));
  }
}
