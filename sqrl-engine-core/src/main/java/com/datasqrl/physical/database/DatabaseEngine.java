package com.datasqrl.physical.database;

import com.datasqrl.config.provider.DatabaseConnectionProvider;
import com.datasqrl.physical.ExecutionEngine;
import com.datasqrl.plan.global.IndexSelectorConfig;

public interface DatabaseEngine extends ExecutionEngine {

    DatabaseConnectionProvider getConnectionProvider();

    IndexSelectorConfig getIndexSelectorConfig();

}
