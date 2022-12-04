package com.datasqrl;

import com.datasqrl.config.EngineSettings;
import com.datasqrl.io.impl.file.DirectoryDataSystemConfig;
import com.datasqrl.io.DataSystemConfig;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.util.DatabaseHandle;
import com.datasqrl.util.TestDataset;
import com.google.common.base.Strings;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;

public abstract class AbstractEngineIT {

  public DatabaseHandle database = null;
  public EngineSettings engineSettings = null;


  @AfterEach
  public void tearDown() {
    if (database != null) {
      database.cleanUp();
      database = null;
    }
  }

  protected void initialize(IntegrationTestSettings settings) {
    Pair<DatabaseHandle, EngineSettings> setup = settings.getSqrlSettings();
    engineSettings = setup.getRight();
    database = setup.getLeft();
  }

  protected DataSystemConfig.DataSystemConfigBuilder getSystemConfigBuilder(
      TestDataset testDataset) {
    DirectoryDataSystemConfig.Discovery.DiscoveryBuilder systemBuilder = DirectoryDataSystemConfig.Discovery.builder()
        .uri(testDataset.getDataDirectory().toUri().getPath());
    if (!Strings.isNullOrEmpty(testDataset.getFilePartPattern())) {
      systemBuilder.partPattern(testDataset.getFilePartPattern());
    }
    DataSystemConfig.DataSystemConfigBuilder builder = DataSystemConfig.builder();
    builder.datadiscovery(systemBuilder.build());
    builder.type(ExternalDataType.SOURCE);
    builder.name(testDataset.getName());
    return builder;
  }

}
