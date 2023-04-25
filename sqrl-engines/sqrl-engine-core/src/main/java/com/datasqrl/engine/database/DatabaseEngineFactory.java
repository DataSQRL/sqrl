package com.datasqrl.engine.database;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.metadata.MetadataStoreProvider;
import lombok.NonNull;

public interface DatabaseEngineFactory extends EngineFactory {

  MetadataStoreProvider getMetadataStore(@NonNull SqrlConfig config);

  @Override
  DatabaseEngine initialize(@NonNull SqrlConfig config);

  default ExecutionEngine.Type getEngineType() {
    return ExecutionEngine.Type.DATABASE;
  }



}
