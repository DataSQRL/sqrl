/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.inference;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.util.data.Retail;
import com.datasqrl.util.data.Retail.RetailScriptNames;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import retrofit2.http.HEAD;

class SchemaInferenceModelTest extends AbstractSchemaInferenceModelTest {

  private Retail example = Retail.INSTANCE;

  @BeforeEach
  public void setup() throws IOException {
    initialize(IntegrationTestSettings.getInMemory(), example.getRootPackageDirectory(), Optional.empty());
  }

  @Test
  public void testC360Inference() {
    initialize(IntegrationTestSettings.getInMemory(), example.getScript(RetailScriptNames.FULL)
        .getRootPackageDirectory(), Optional.empty());

    this.inferSchemaAndQueries(
        example.getScript(RetailScriptNames.FULL),
        Path.of("src/test/resources/c360bundle/schema.full.graphqls"));
    SqrlSchema schema = injector.getInstance(SqrlSchema.class);
    assertEquals(336, schema.getQueries().size());
  }
}