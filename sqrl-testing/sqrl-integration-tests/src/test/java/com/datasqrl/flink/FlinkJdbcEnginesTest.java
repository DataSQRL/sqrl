package com.datasqrl.flink;

import com.datasqrl.AbstractQuerySQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.IntegrationTestSettings.DatabaseEngine;
import com.datasqrl.IntegrationTestSettings.StreamEngine;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestGraphQLSchema;
import com.datasqrl.util.TestScript;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import java.util.Optional;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class FlinkJdbcEnginesTest extends AbstractQuerySQRLIT {

  @ParameterizedTest
  @ArgumentsSource(TestScript.AllScriptsWithAllEnginesProvider.class)
  public void fullScriptTest(TestScript script, TestGraphQLSchema graphQLSchema,
      DatabaseEngine engine,
      Vertx vertx,
      VertxTestContext testContext) {
    this.vertx = vertx;
    this.vertxContext = testContext;
    snapshot = SnapshotTest.Snapshot.of(getClass(), script.getName(), graphQLSchema.getName(),
        engine.name());
    initialize(IntegrationTestSettings.getFlinkWithDB(engine),
        script.getRootPackageDirectory(), Optional.empty());
    validateSchemaAndQueries(script.getScript(), graphQLSchema.getSchema(),
        graphQLSchema.getQueries());
  }

}
