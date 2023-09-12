/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util.data;

import com.datasqrl.io.impl.file.FileDataSystemConfig;
import com.datasqrl.io.impl.file.FileDataSystemFactory;
import com.datasqrl.io.tables.TableConfig;
import java.nio.file.Path;
import java.util.Set;

public class Books extends UseCaseExample {

  public static final Books INSTANCE = new Books();

  public static final Path DATA_DIRECTORY = Path.of("/Users/matthias/Data/books/sample");

  protected Books() {
    super(Set.of("books","authors","reviews"), scripts()
        .add("recommendation", "")
        .build());
  }

  @Override
  public TableConfig getDiscoveryConfig() {
    return FileDataSystemFactory.getFileDiscoveryConfig(getName(),
            FileDataSystemConfig.builder()
                .directoryURI(getDataDirectory().toString())
                .monitorIntervalMs("0")
                .build())
        .build();
  }

  @Override
  public Path getDataDirectory() {
    return DATA_DIRECTORY;
  }

}
