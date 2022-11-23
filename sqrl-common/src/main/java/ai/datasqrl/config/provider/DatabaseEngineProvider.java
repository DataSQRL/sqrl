package ai.datasqrl.config.provider;

import lombok.NonNull;

public interface DatabaseEngineProvider {

    DatabaseConnectionProvider getDatabase(@NonNull String databaseName);

}
