package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.execution.StreamEngine;
import ai.dataeng.sqml.execution.flink.environment.FlinkStreamEngine;
import ai.dataeng.sqml.io.sources.dataset.SourceTable;

public interface StreamMonitorProvider {

    StreamEngine.SourceMonitor create(StreamEngine engine, JDBCConnectionProvider jdbc, MetadataStoreProvider metaProvider,
                                      SerializerProvider serializerProvider,
                                      DatasetRegistryPersistenceProvider registryProvider);

}
