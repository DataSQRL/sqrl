/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.discovery;

import com.datasqrl.io.DataSystemConfig;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.impl.file.DirectoryDataSystemConfig;
import com.google.common.base.Preconditions;

import java.nio.file.Files;
import java.nio.file.Path;

public class DiscoveryUtil {

  public static DataSystemConfig.DataSystemConfigBuilder getDirectorySystemConfig(Path dir) {
    Preconditions.checkArgument(Files.isDirectory(dir));
    DirectoryDataSystemConfig.Discovery.DiscoveryBuilder systemBuilder = DirectoryDataSystemConfig.Discovery.builder()
        .uri(dir.toUri().getPath());
    DataSystemConfig.DataSystemConfigBuilder builder = DataSystemConfig.builder();
    builder.datadiscovery(systemBuilder.build());
    builder.type(ExternalDataType.SOURCE);
    builder.name(dir.getFileName().toString());
    return builder;
  }

}
