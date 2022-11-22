package ai.datasqrl.flink;

import ai.datasqrl.AbstractPhysicalSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.util.SnapshotTest;
import ai.datasqrl.util.TestScript;
import ai.datasqrl.util.data.Nutshop;
import com.google.common.collect.ImmutableSet;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Set;

public class FlinkPhysicalUseCaseTest extends AbstractPhysicalSQRLIT {

    @BeforeEach
    public void setup(TestInfo testInfo) throws IOException {
        this.snapshot = SnapshotTest.Snapshot.of(getClass(),testInfo);
    }

    @SneakyThrows
    private void scriptTest(TestScript script, boolean removeTimestamps, boolean snapshotData) {
        initialize(IntegrationTestSettings.getFlinkWithDB(), script.getRootPackageDirectory());
        validateTables(Files.readString(script.getScriptPath()), script.getResultTables(),
                removeTimestamps?ImmutableSet.copyOf(script.getResultTables()): Set.of(),
                snapshotData?Set.of():ImmutableSet.copyOf(script.getResultTables()));
    }

    @ParameterizedTest
    @ArgumentsSource(TestScript.AllScriptsProvider.class)
    public void fullScriptTest(TestScript script) {
        scriptTest(script,true, script.dataSnapshot());
    }

    @Test
    @Disabled
    public void forDebuggingIndividualUseCases() {
        scriptTest(Nutshop.MEDIUM.getScripts().get(1), false, false);
    }

}
