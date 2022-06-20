package ai.datasqrl;

import ai.datasqrl.config.EnvironmentConfiguration;
import ai.datasqrl.config.GlobalConfiguration;
import ai.datasqrl.config.SqrlSettings;
import ai.datasqrl.config.engines.FlinkConfiguration;
import ai.datasqrl.config.engines.InMemoryStreamConfiguration;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.util.TestDatabase;
import lombok.Builder;
import lombok.Value;
import org.apache.commons.lang3.tuple.Pair;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Value
@Builder
public class IntegrationTestSettings {

    public enum StreamEngine {FLINK, INMEMORY}

    @Builder.Default
    final StreamEngine engine = StreamEngine.INMEMORY;
    @Builder.Default
    final boolean monitorSources = true;

    Pair<TestDatabase, SqrlSettings> getSqrlSettings() {
        TestDatabase database = new TestDatabase();
        GlobalConfiguration.Engines.EnginesBuilder enginesBuilder = GlobalConfiguration.Engines.builder();
        //Database
        enginesBuilder.jdbc(database.getJdbcConfiguration());
        //Stream engine
        if (getEngine() == IntegrationTestSettings.StreamEngine.FLINK) {
            enginesBuilder.flink(new FlinkConfiguration());
        } else {
            enginesBuilder.inmemory(new InMemoryStreamConfiguration());
        }

        GlobalConfiguration config = GlobalConfiguration.builder()
                .engines(enginesBuilder.build())
                .environment(EnvironmentConfiguration.builder()
                        .monitorSources(isMonitorSources())
                        .metastore(EnvironmentConfiguration.MetaData.builder()
                                .database(EnvironmentConfiguration.MetaData.DEFAULT_DATABASE)
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


    public static IntegrationTestSettings getDefault() {
        return IntegrationTestSettings.builder().build();
    }

    public static IntegrationTestSettings getDefault(boolean monitorSources) {
        return IntegrationTestSettings.builder().monitorSources(monitorSources).build();
    }

    public static IntegrationTestSettings getFlink() {
        return IntegrationTestSettings.builder().engine(StreamEngine.FLINK).build();
    }


}
