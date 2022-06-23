package ai.datasqrl.config.engines;

import ai.datasqrl.config.provider.DatabaseConnectionProvider;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileDatabaseConfiguration implements EngineConfiguration.Database {

    @NonNull @NotNull @NotEmpty
    String directory;

    @Override
    public DatabaseConnectionProvider getDatabase(@NonNull String databaseName) {
        Preconditions.checkArgument(StringUtils.isAlphanumeric(databaseName),"Invalid database name: %s", databaseName);
        Path p = Paths.get(directory);
        return new ConnectionProvider(p.resolve(databaseName));
    }

    @Value
    public static class ConnectionProvider implements DatabaseConnectionProvider {
        Path directory;
    }
}
