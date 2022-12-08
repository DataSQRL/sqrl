/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;

import com.datasqrl.io.DataSystemConfig;
import com.datasqrl.io.DataSystemDiscoveryConfig;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.formats.FileFormat;
import com.datasqrl.io.impl.file.DirectoryDataSystemConfig;
import com.datasqrl.loaders.DataSource;
import com.datasqrl.util.data.Retail;
import java.nio.file.Path;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class GenerateDataSystemConfig {

  @Test
  @Disabled
  @SneakyThrows
  public void generateConfigFile() {
    TestDataset testDataset = Retail.INSTANCE;
    Path output = testDataset.getRootPackageDirectory().resolve("export-data");
    DataSystemDiscoveryConfig datasystem = DirectoryDataSystemConfig.ofDirectory(output);
    DataSystemConfig.DataSystemConfigBuilder builder = DataSystemConfig.builder();
    builder.datadiscovery(datasystem);
    builder.type(ExternalDataType.sink);
    builder.name("output");
    builder.format(FileFormat.JSON.getImplementation().getDefaultConfiguration());
    DataSystemConfig config = builder.build();

    Path datasystemConfigFile = testDataset.getRootPackageDirectory().resolve("output")
        .resolve(DataSource.DATASYSTEM_FILE);
    FileTestUtil.writeJson(datasystemConfigFile, config);
  }

}
