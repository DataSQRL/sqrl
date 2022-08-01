package ai.datasqrl;

import ai.datasqrl.config.SqrlSettings;
import ai.datasqrl.environment.Environment;
import ai.datasqrl.io.sinks.registry.DataSinkRegistry;
import ai.datasqrl.io.sources.dataset.DatasetRegistry;
import ai.datasqrl.util.DatabaseHandle;
import graphql.com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;

public abstract class AbstractSQRLIT {

    private DatabaseHandle database = null;
    public SqrlSettings sqrlSettings = null;
    public Environment env = null;
    public DatasetRegistry sourceRegistry = null;
    public DataSinkRegistry sinkRegistry = null;

    @AfterEach
    public void tearDown() {
        if (database!=null) {
            database.cleanUp();
            database = null;
        }
        if (env!=null) {
            env.close();
            env = null;
            sourceRegistry = null;
            sinkRegistry = null;
        }
    }

    protected void initialize(IntegrationTestSettings settings) {
        Pair<DatabaseHandle, SqrlSettings> setup = settings.getSqrlSettings();
        sqrlSettings = setup.getRight();
        database = setup.getLeft();
        env = Environment.create(sqrlSettings);
        sourceRegistry = env.getDatasetRegistry();
        sinkRegistry = env.getDataSinkRegistry();
    }








}
