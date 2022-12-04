/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database;

import com.datasqrl.config.provider.DatabaseConnectionProvider;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.plan.global.IndexSelectorConfig;

public interface DatabaseEngine extends ExecutionEngine {

  DatabaseConnectionProvider getConnectionProvider();

  IndexSelectorConfig getIndexSelectorConfig();

}
