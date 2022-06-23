package ai.datasqrl.config.provider;

import ai.datasqrl.config.engines.JDBCConfiguration;
import lombok.NonNull;

public interface DatabaseEngineProvider {

    DatabaseConnectionProvider getDatabase(@NonNull String databaseName);

}
