/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.metadata.MetadataStoreProvider;
import com.datasqrl.engine.EngineConfiguration;
import com.datasqrl.engine.ExecutionEngine;
import lombok.NonNull;

/**
 * TODO:remove, replaced with DatabaseEngineFactory
 */
public interface DatabaseEngineConfiguration extends EngineConfiguration {

  MetadataStoreProvider getMetadataStore();

  @Override
  DatabaseEngine initialize(@NonNull ErrorCollector errors);

  default ExecutionEngine.Type getEngineType() {
    return ExecutionEngine.Type.DATABASE;
  }

}
