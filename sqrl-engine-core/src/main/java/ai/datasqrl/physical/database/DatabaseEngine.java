package ai.datasqrl.physical.database;

import ai.datasqrl.config.provider.DatabaseConnectionProvider;
import ai.datasqrl.physical.ExecutionEngine;
import ai.datasqrl.plan.global.IndexSelectorConfig;

public interface DatabaseEngine extends ExecutionEngine {

    DatabaseConnectionProvider getConnectionProvider();

    IndexSelectorConfig getIndexSelectorConfig();

}
