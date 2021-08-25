package ai.dataeng.sqml.execution;

import ai.dataeng.sqml.db.keyvalue.HierarchyKeyValueStore;
import ai.dataeng.sqml.flink.DefaultEnvironmentFactory;
import ai.dataeng.sqml.flink.EnvironmentFactory;
import ai.dataeng.sqml.ingest.DataSourceRegistry;
import ai.dataeng.sqml.source.SourceDataset;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SQMLServer {

    private final HierarchyKeyValueStore.Factory metadataStoreFactory;
    private final DataSourceRegistry registry;

    private final EnvironmentFactory envProvider = new DefaultEnvironmentFactory();

    public SQMLServer(HierarchyKeyValueStore.Factory metadataStoreFactory) {
        this.metadataStoreFactory = metadataStoreFactory;
        this.registry = new DataSourceRegistry(metadataStoreFactory);
    }

    public void addDataSet(SourceDataset dataset) {
        registry.addDataset(dataset);
    }

    public void run(Bundle sqml) throws Exception {
        StreamExecutionEnvironment flinkEnv = envProvider.create();
        compile(sqml,flinkEnv);
        //4. Store bundle meta data and execute Flink Job
        flinkEnv.execute("somename");
    }

    public void compile(Bundle sqml, StreamExecutionEnvironment flinkEnv) {
        //1. Parse

        //2. Analyze: resolve import dependencies, data types, and view definitions

        //3. Construct Flink Job
    }

    public void start() {
        //Restore bundles from meta data
        //Recover all Flink jobs from savepoint
    }

    public void shutdown() {
        //Savepoint Flink Jobs and shut down gracefully
    }
}
