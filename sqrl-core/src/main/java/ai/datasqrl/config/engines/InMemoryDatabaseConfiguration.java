package ai.datasqrl.config.engines;

import ai.datasqrl.config.provider.DatabaseConnectionProvider;
import lombok.NonNull;
import lombok.Value;

public class InMemoryDatabaseConfiguration implements EngineConfiguration.Database {

    @Override
    public ConnectionProvider getDatabase(@NonNull String databaseName) {
        return new ConnectionProvider(databaseName);
    }

    @Value
    public static class ConnectionProvider implements DatabaseConnectionProvider {
        String databaseName;
    }

}
