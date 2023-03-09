/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import com.datasqrl.config.EngineSettings;
import com.datasqrl.io.DataSystemConfig;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.util.DatabaseHandle;
import com.datasqrl.util.TestDataset;
import com.google.inject.Injector;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;

public abstract class AbstractEngineIT {

  public DatabaseHandle database = null;
  public EngineSettings engineSettings = null;
  protected Injector injector;


  @AfterEach
  public void tearDown() {
    if (database != null) {
      database.cleanUp();
      database = null;
    }
  }

  protected void initialize(IntegrationTestSettings settings, Injector injector) {
    this.initialize(settings);
    this.injector = injector;
  }
  protected void initialize(IntegrationTestSettings settings) {
    if (engineSettings == null) {
      Pair<DatabaseHandle, EngineSettings> setup = settings.getSqrlSettings();
      engineSettings = setup.getRight();
      database = setup.getLeft();
    }
  }

  protected DataSystemConfig.DataSystemConfigBuilder getSystemConfigBuilder(
      TestDataset testDataset) {
    DataSystemConfig.DataSystemConfigBuilder builder = DataSystemConfig.builder();
    builder.datadiscovery(testDataset.getDiscoveryConfig());
    builder.type(ExternalDataType.source);
    builder.name(testDataset.getName());
    return builder;
  }

}
