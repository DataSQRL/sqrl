/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database;

import com.datasqrl.config.EngineType;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.engine.EngineConfiguration;
import lombok.NonNull;

/**
 * TODO:remove, replaced with DatabaseEngineFactory
 */
public interface DatabaseEngineConfiguration extends EngineConfiguration {


  @Override
  DatabaseEngine initialize(@NonNull ErrorCollector errors);

  default EngineType getEngineType() {
    return EngineType.DATABASE;
  }

}
