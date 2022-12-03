package com.datasqrl.physical.database;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.metadata.MetadataStoreProvider;
import com.datasqrl.physical.EngineConfiguration;
import com.datasqrl.physical.ExecutionEngine;
import lombok.NonNull;

public interface DatabaseEngineConfiguration extends EngineConfiguration {

    MetadataStoreProvider getMetadataStore();

    @Override
    DatabaseEngine initialize(@NonNull ErrorCollector errors);

    default ExecutionEngine.Type getEngineType() {
        return ExecutionEngine.Type.DATABASE;
    }

}
