package ai.datasqrl;

import ai.datasqrl.config.EnvironmentConfiguration;
import ai.datasqrl.config.GlobalConfiguration;
import ai.datasqrl.config.SqrlSettings;
import ai.datasqrl.config.engines.FlinkConfiguration;
import ai.datasqrl.config.engines.InMemoryStreamConfiguration;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.environment.Environment;
import ai.datasqrl.io.sinks.registry.DataSinkRegistry;
import ai.datasqrl.io.sources.dataset.DatasetRegistry;
import ai.datasqrl.util.TestDatabase;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class AbstractSQRLIntegrationTest {

    private TestDatabase testDatabase = null;
    public SqrlSettings sqrlSettings = null;
    public Environment env = null;
    public DatasetRegistry sourceRegistry = null;
    public DataSinkRegistry sinkRegistry = null;

    @AfterEach
    public void tearDown() {
        if (testDatabase!=null) {
            testDatabase.stop();
            testDatabase = null;
        }
        if (env!=null) {
            env.close();
            env = null;
            sourceRegistry = null;
            sinkRegistry = null;
        }
    }

    protected void initialize(IntegrationTestSettings settings) {
        Pair<TestDatabase, SqrlSettings> setup = settings.getSqrlSettings();
        sqrlSettings = setup.getRight();
        testDatabase = setup.getLeft();
        env = Environment.create(sqrlSettings);
        sourceRegistry = env.getDatasetRegistry();
        sinkRegistry = env.getDataSinkRegistry();
    }








}
