package ai.datasqrl;

import ai.datasqrl.config.EnvironmentConfiguration;
import ai.datasqrl.config.GlobalConfiguration;
import ai.datasqrl.config.SqrlSettings;
import ai.datasqrl.config.engines.FlinkConfiguration;
import ai.datasqrl.config.engines.InMemoryDatabaseConfiguration;
import ai.datasqrl.config.engines.InMemoryStreamConfiguration;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.metadata.InMemoryMetadataStore;
import ai.datasqrl.util.DatabaseHandle;
import ai.datasqrl.util.JDBCTestDatabase;
import lombok.Builder;
import lombok.Value;
import org.apache.commons.lang3.tuple.Pair;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Value
@Builder
public class IntegrationTestSettings {

    public enum StreamEngine {FLINK, INMEMORY}

    public enum DatabaseEngine {INMEMORY, H2, POSTGRES, LOCAL}

    @Builder.Default
    final StreamEngine stream = StreamEngine.INMEMORY;
    @Builder.Default
    final DatabaseEngine database = DatabaseEngine.INMEMORY;
    @Builder.Default
    final boolean monitorSources = true;

    Pair<DatabaseHandle, SqrlSettings> getSqrlSettings() {

        GlobalConfiguration.Engines.EnginesBuilder enginesBuilder = GlobalConfiguration.Engines.builder();
        //Database

        //Stream engine
        switch (getStream()) {
            case FLINK:
                enginesBuilder.flink(new FlinkConfiguration());
                break;
            case INMEMORY:
                enginesBuilder.inmemoryStream(new InMemoryStreamConfiguration());
                break;
        }
        DatabaseHandle database = null;
        switch (getDatabase()) {
            case INMEMORY:
                enginesBuilder.inmemoryDB(new InMemoryDatabaseConfiguration());
                database = () -> InMemoryMetadataStore.clearLocal();
                break;
            case H2:
            case POSTGRES:
            case LOCAL:
                JDBCTestDatabase jdbcDB = new JDBCTestDatabase(getDatabase());
                enginesBuilder.jdbc(jdbcDB.getJdbcConfiguration());
                database = jdbcDB;
        }

        GlobalConfiguration config = GlobalConfiguration.builder()
                .engines(enginesBuilder.build())
                .environment(EnvironmentConfiguration.builder()
                        .monitorSources(isMonitorSources())
                        .metastore(EnvironmentConfiguration.MetaData.builder()
                                .databaseName(EnvironmentConfiguration.MetaData.DEFAULT_DATABASE)
                                .build())
                        .build())
                .build();
        validateConfig(config);
        return Pair.of(database,SqrlSettings.fromConfiguration(config));
    }

    public static void validateConfig(GlobalConfiguration config) {
        ErrorCollector errors = ErrorCollector.root();
        assertTrue(config.validate(errors), errors.toString());
    }


    public static IntegrationTestSettings getInMemory() {
        return IntegrationTestSettings.builder().build();
    }

    public static IntegrationTestSettings getInMemory(boolean monitorSources) {
        return IntegrationTestSettings.builder().monitorSources(monitorSources).build();
    }

    public static IntegrationTestSettings getFlinkWithDB() {
        return getFlinkWithDB(false);
    }

    public static IntegrationTestSettings getFlinkWithDB(boolean monitorSources) {
        return getEngines(StreamEngine.FLINK,DatabaseEngine.POSTGRES,monitorSources);
    }

    public static IntegrationTestSettings getEngines(StreamEngine stream, DatabaseEngine database) {
        return getEngines(stream,database,false);
    }

    public static IntegrationTestSettings getEngines(StreamEngine stream, DatabaseEngine database, boolean monitorSources) {
        return IntegrationTestSettings.builder().stream(stream).database(database)
            .monitorSources(monitorSources).build();
    }

    @Value
    public static class EnginePair {

        DatabaseEngine database;
        StreamEngine stream;

    }

}
