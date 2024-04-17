/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database;

import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.engine.EngineConfiguration;
import com.datasqrl.engine.ExecutionEngine;
import lombok.NonNull;

/**
 * TODO:remove, replaced with DatabaseEngineFactory
 */
public interface DatabaseEngineConfiguration extends EngineConfiguration {


  @Override
  DatabaseEngine initialize(@NonNull ErrorCollector errors);

  default Type getEngineType() {
    return Type.DATABASE;
  }

}
