/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.compiler;

import com.datasqrl.AbstractEngineIT;
import com.datasqrl.IntegrationTestSettings;
import java.util.Optional;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ConfigurationIT extends AbstractEngineIT {

  @Test
  public void testSettings() {
    initialize(IntegrationTestSettings.getInMemory(), null, Optional.empty());
    assertNotNull(database);
    assertNotNull(pipelineFactory);
  }
}
