/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;

import com.datasqrl.io.impl.file.FileDataSystemFactory;
import com.datasqrl.io.tables.TableConfig;
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

    TableConfig.Builder sinkConfig = FileDataSystemFactory.getFileSinkConfig(output);


    Path datasystemConfigFile = testDataset.getRootPackageDirectory().resolve("output")
        .resolve(DataSource.DATASYSTEM_FILE_PREFIX + "discover" + DataSource.TABLE_FILE_SUFFIX);
    sinkConfig.build().toFile(datasystemConfigFile);
  }

}
