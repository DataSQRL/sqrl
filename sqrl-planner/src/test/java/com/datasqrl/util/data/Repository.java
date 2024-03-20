/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util.data;

import com.datasqrl.io.impl.file.FileDataSystemConfig;
import com.datasqrl.io.impl.file.FileDataSystemFactory;
import com.datasqrl.io.tables.TableConfig;
import java.util.Set;

public class Repository extends UseCaseExample {

  public static final Repository INSTANCE = new Repository();

  protected Repository() {
    super(Set.of("package"), script("repo", "package", "submission"));
  }

  @Override
  public TableConfig getDiscoveryConfig() {
    return FileDataSystemFactory.getFileDiscoveryConfig(getName(),
        FileDataSystemConfig.builder()
            .directoryURI(getDataDirectory().toUri().getPath())
            .filenamePattern("([^\\.]+?)\\.(?:[-_A-Za-z0-9]+)")
            .build()).build();
  }
}
