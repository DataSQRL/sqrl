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

    Pair<Object, APIConnectorManager> result = this.inferSchemaAndQueries(
        example.getScript(RetailScriptNames.FULL),
        Path.of("src/test/resources/c360bundle/schema.full.graphqls"));
    assertEquals(336, result.getValue().getQueries().size());

//    APIConnectorManager instance = injector.getInstance(APIConnectorManager.class);
//    InferredSchema result = inferSchemaAndQueries(
//        example.getScript(RetailScriptNames.FULL),
//        Path.of("src/test/resources/c360bundle/schema.full.graphqls"));
//    assertEquals(60, result.getQuery().getFields().size());
//    assertEquals(336, instance.getQueries().size());
//>>>>>>> 8622af179 (Reuse more code)
  }
}