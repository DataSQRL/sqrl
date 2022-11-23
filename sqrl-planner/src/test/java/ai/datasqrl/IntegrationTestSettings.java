package ai.datasqrl;

import ai.datasqrl.config.DiscoveryConfiguration;
import ai.datasqrl.config.GlobalConfiguration;
import ai.datasqrl.config.SqrlSettings;
import ai.datasqrl.config.SqrlSettings.SqrlSettingsBuilder;
import ai.datasqrl.config.engines.FlinkConfiguration;
import ai.datasqrl.config.engines.InMemoryDatabaseConfiguration;
import ai.datasqrl.config.engines.InMemoryStreamConfiguration;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.metadata.FileMetadataStore;
import ai.datasqrl.config.metadata.InMemoryMetadataStore;
import ai.datasqrl.config.metadata.JDBCMetadataStore;
import ai.datasqrl.config.metadata.MetadataNamedPersistence;
import ai.datasqrl.config.serializer.KryoProvider;
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
                .discovery(DiscoveryConfiguration.builder()
                        .metastore(DiscoveryConfiguration.MetaData.builder()
                                .databaseName(DiscoveryConfiguration.MetaData.DEFAULT_DATABASE)
                                .build())
                        .build())
                .build();
        validateConfig(config);
        return Pair.of(database,fromConfiguration(config));
    }


    public static SqrlSettings fromConfiguration(GlobalConfiguration config) {
        return builderFromConfiguration(config).build();
    }

    public static SqrlSettingsBuilder builderFromConfiguration(GlobalConfiguration config) {
        SqrlSettingsBuilder builder = SqrlSettings.builder()
            .discoveryConfiguration(config.getDiscovery())
            .serializerProvider(new KryoProvider());

        GlobalConfiguration.Engines engines = config.getEngines();
        if (engines.getFlink() != null) {
            builder.streamEngineProvider(engines.getFlink());
        } else if (engines.getInmemoryStream() != null) {
            builder.streamEngineProvider(engines.getInmemoryStream());
        } else throw new IllegalArgumentException("Must configure a stream engine");

        if (engines.getJdbc() != null) {
            builder.databaseEngineProvider(config.getEngines().getJdbc());
            builder.metadataStoreProvider(new JDBCMetadataStore.Provider());
        } else if (engines.getInmemoryDB() != null) {
            builder.databaseEngineProvider(config.getEngines().getInmemoryDB());
            builder.metadataStoreProvider(new InMemoryMetadataStore.Provider());
        } else if (engines.getFileDB() != null) {
            builder.databaseEngineProvider(config.getEngines().getFileDB());
            builder.metadataStoreProvider(new FileMetadataStore.Provider());
        }else throw new IllegalArgumentException("Must configure a database engine");


        return builder;
    }


    public static void validateConfig(GlobalConfiguration config) {
        ErrorCollector errors = ErrorCollector.root();
        assertTrue(config.validate(errors), errors.toString());
    }


    public static IntegrationTestSettings getInMemory() {
        return IntegrationTestSettings.builder().build();
    }

    public static IntegrationTestSettings getFlinkWithDB() {
        return getEngines(StreamEngine.FLINK,DatabaseEngine.POSTGRES);
    }

    public static IntegrationTestSettings getEngines(StreamEngine stream, DatabaseEngine database) {
        return IntegrationTestSettings.builder().stream(stream).database(database).build();
    }

    @Value
    public static class EnginePair {

        DatabaseEngine database;
        StreamEngine stream;

        public String getName() {
            return database.name() + "_" + stream.name();
        }

    }

}
