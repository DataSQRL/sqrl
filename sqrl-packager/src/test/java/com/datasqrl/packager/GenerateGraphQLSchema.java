/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager;

import com.datasqrl.graphql.generate.SchemaGeneratorUseCaseTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class GenerateGraphQLSchema extends SchemaGeneratorUseCaseTest {

  @Test
  public void writeSchemaFile() {
    produceSchemaFile(DataSQRL.INSTANCE.getScript());
  }
}
