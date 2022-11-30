package ai.datasqrl.physical.database;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.metadata.MetadataStoreProvider;
import ai.datasqrl.physical.EngineConfiguration;
import ai.datasqrl.physical.ExecutionEngine;
import lombok.NonNull;

public interface DatabaseEngineConfiguration extends EngineConfiguration {

    MetadataStoreProvider getMetadataStore();

    @Override
    DatabaseEngine initialize(@NonNull ErrorCollector errors);

    default ExecutionEngine.Type getEngineType() {
        return ExecutionEngine.Type.DATABASE;
    }

}
