package ai.datasqrl;

import ai.datasqrl.config.EngineSettings;
import ai.datasqrl.metadata.MetadataConfiguration;
import ai.datasqrl.physical.EngineConfiguration;
import ai.datasqrl.physical.database.inmemory.InMemoryDatabaseConfiguration;
import ai.datasqrl.physical.database.inmemory.InMemoryMetadataStore;
import ai.datasqrl.physical.stream.flink.FlinkEngineConfiguration;
import ai.datasqrl.physical.stream.inmemory.InMemoryStreamConfiguration;
import ai.datasqrl.util.DatabaseHandle;
import ai.datasqrl.util.JDBCTestDatabase;
import lombok.Builder;
import lombok.Value;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

@Value
@Builder
public class IntegrationTestSettings {

    public enum StreamEngine {FLINK, INMEMORY}

    public enum DatabaseEngine {INMEMORY, H2, POSTGRES, LOCAL}

    @Builder.Default
    final StreamEngine stream = StreamEngine.INMEMORY;
    @Builder.Default
    final DatabaseEngine database = DatabaseEngine.INMEMORY;

    Pair<DatabaseHandle, EngineSettings> getSqrlSettings() {

        List<EngineConfiguration> engines = new ArrayList<>();
        //Stream engine
        switch (getStream()) {
            case FLINK:
                engines.add(new FlinkEngineConfiguration());
                break;
            case INMEMORY:
                engines.add(new InMemoryStreamConfiguration());
                break;
        }
        //Database engine
        DatabaseHandle database = null;
        switch (getDatabase()) {
            case INMEMORY:
                engines.add(new InMemoryDatabaseConfiguration());
                database = () -> InMemoryMetadataStore.clearLocal();
                break;
            case H2:
            case POSTGRES:
            case LOCAL:
                JDBCTestDatabase jdbcDB = new JDBCTestDatabase(getDatabase());
                engines.add(jdbcDB.getJdbcConfiguration());
                database = jdbcDB;
        }
        return Pair.of(database,new EngineSettings(engines, new MetadataConfiguration()));
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
