package com.datasqrl.flink;

import com.datasqrl.AbstractQuerySQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestGraphQLSchema;
import com.datasqrl.util.TestScript;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Disabled;
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
}
