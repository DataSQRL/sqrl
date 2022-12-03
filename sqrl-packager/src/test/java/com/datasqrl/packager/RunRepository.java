package ai.datasqrl.packager;

import ai.datasqrl.AbstractQuerySQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.util.SnapshotTest;
import ai.datasqrl.util.TestGraphQLSchema;
import ai.datasqrl.util.TestScript;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class RunRepository extends AbstractQuerySQRLIT {

    @Test
    public void testQuery(Vertx vertx, VertxTestContext testContext) {
        fullScriptTest(DataSQRL.INSTANCE.getScript(), DataSQRL.INSTANCE.getGraphQL(), vertx, testContext);
    }

    public void fullScriptTest(TestScript script, TestGraphQLSchema graphQLSchema, Vertx vertx, VertxTestContext testContext) {
        this.vertx = vertx;
        this.vertxContext = testContext;
        snapshot = SnapshotTest.Snapshot.of(getClass(), script.getName(), graphQLSchema.getName());
        initialize(IntegrationTestSettings.getFlinkWithDB(), script.getRootPackageDirectory());
        validateSchemaAndQueries(script.getScript(), graphQLSchema.getSchema(), graphQLSchema.getQueries());
    }
}
