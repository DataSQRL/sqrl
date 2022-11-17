package ai.datasqrl.flink;

import ai.datasqrl.AbstractPhysicalSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.util.SnapshotTest;
import ai.datasqrl.util.TestGraphQLSchema;
import ai.datasqrl.util.TestScript;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class FlinkQueryUseCaseTest extends AbstractPhysicalSQRLIT {

    @ParameterizedTest
    @Disabled
    @ArgumentsSource(TestScript.AllScriptsWithGraphQLSchemaProvider.class)
    public void fullScriptTest(TestScript script, TestGraphQLSchema graphQLSchema) {
        snapshot = SnapshotTest.Snapshot.of(getClass(), script.getName(), graphQLSchema.getName());
        initialize(IntegrationTestSettings.getFlinkWithDB(), script.getRootPackageDirectory());
        validateSchemaAndQueries(script.getScript(), graphQLSchema.getSchema(), graphQLSchema.getQueries());
    }

}
