package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.execution.StreamEngine;

public interface StreamMonitorProvider {

    StreamEngine.SourceMonitor create(StreamEngine engine, JDBCConnectionProvider jdbc, MetadataStoreProvider metaProvider,
                                      SerializerProvider serializerProvider,
                                      DatasetRegistryPersistenceProvider registryProvider);

}
