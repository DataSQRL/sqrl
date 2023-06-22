/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import com.datasqrl.config.PipelineFactory;
import com.datasqrl.util.DatabaseHandle;
import com.google.inject.Injector;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;

public abstract class AbstractEngineIT {

  public DatabaseHandle database = null;
  protected Injector injector;


  @AfterEach
  public void tearDown() {
    if (database != null) {
      database.cleanUp();
      database = null;
    }
  }

  protected PipelineFactory initialize(IntegrationTestSettings settings, Injector injector) {
    this.injector = injector;
    return this.initialize(settings);
  }

  protected PipelineFactory initialize(IntegrationTestSettings settings) {
    if (database == null) {
      Pair<DatabaseHandle, PipelineFactory> setup = settings.getSqrlSettings();
      database = setup.getLeft();
      return setup.getRight();
    } else {
      return null;
    }
  }
}
