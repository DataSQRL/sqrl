/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flink;

import com.datasqrl.AbstractQuerySQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestGraphQLSchema;
import com.datasqrl.util.TestScript;
import com.datasqrl.util.data.DataSQRLRepo;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class FlinkQueryUseCaseTest extends AbstractQuerySQRLIT {

  @ParameterizedTest
  @ArgumentsSource(TestScript.AllScriptsWithGraphQLSchemaProvider.class)
  public void fullScriptTest(TestScript script, TestGraphQLSchema graphQLSchema, Vertx vertx,
      VertxTestContext testContext) {
    this.vertx = vertx;
    this.vertxContext = testContext;
    snapshot = SnapshotTest.Snapshot.of(getClass(), script.getName(), graphQLSchema.getName());
    initialize(IntegrationTestSettings.getFlinkWithDB(), script.getRootPackageDirectory());
    validateSchemaAndQueries(script.getScript(), graphQLSchema.getSchema(),
        graphQLSchema.getQueries());
  }

  @Disabled
  @Test
  public void runSpecificTest(Vertx vertx,
      VertxTestContext testContext) {
    TestScript repo = DataSQRLRepo.INSTANCE.getScript();
    fullScriptTest(repo, repo.getGraphQLSchemas().get(0), vertx, testContext);
  }
}
