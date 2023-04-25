/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.compiler;

import com.datasqrl.AbstractEngineIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.config.PipelineFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ConfigurationIT extends AbstractEngineIT {

  @Test
  public void testSettings() {
    PipelineFactory pipelineFactory = initialize(IntegrationTestSettings.getInMemory());
    assertNotNull(database);
    assertNotNull(pipelineFactory);
  }


}
