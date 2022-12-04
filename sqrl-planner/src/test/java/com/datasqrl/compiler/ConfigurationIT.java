package com.datasqrl.compiler;

import com.datasqrl.AbstractEngineIT;
import com.datasqrl.IntegrationTestSettings;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ConfigurationIT extends AbstractEngineIT {

  @Test
  public void testSettings() {
    initialize(IntegrationTestSettings.getInMemory());
    assertNotNull(database);
    assertNotNull(engineSettings);
  }


}
