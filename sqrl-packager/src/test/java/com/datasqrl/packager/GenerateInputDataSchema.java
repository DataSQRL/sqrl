/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager;

import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.io.TestDataSetMonitoringIT;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class GenerateInputDataSchema extends TestDataSetMonitoringIT {

  @Test
  public void generateSchema() {
    generateTableConfigAndSchemaInDataDir(DataSQRL.INSTANCE, IntegrationTestSettings.getInMemory());
  }


}
