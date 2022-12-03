package com.datasqrl;

import com.datasqrl.config.EngineSettings;
import com.datasqrl.config.GlobalEngineConfiguration;
import com.datasqrl.config.error.ErrorCollector;
import com.datasqrl.physical.EngineConfiguration;
import com.datasqrl.physical.database.inmemory.InMemoryDatabaseConfiguration;
import com.datasqrl.physical.database.inmemory.InMemoryMetadataStore;
import com.datasqrl.physical.stream.flink.FlinkEngineConfiguration;
import com.datasqrl.physical.stream.inmemory.InMemoryStreamConfiguration;
import com.datasqrl.util.DatabaseHandle;
import com.datasqrl.util.JDBCTestDatabase;
import lombok.Builder;
import lombok.Value;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

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
        GlobalEngineConfiguration engineConfig = GlobalEngineConfiguration.builder().engines(engines).build();
        ErrorCollector errors = ErrorCollector.root();
        EngineSettings engineSettings = engineConfig.initializeEngines(errors);
        assertNotNull(engineSettings, errors.toString());
        return Pair.of(database,engineSettings);
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
