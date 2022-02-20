package ai.dataeng.sqml.api;

import ai.dataeng.sqml.Environment;
import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.config.EnvironmentConfiguration;
import ai.dataeng.sqml.config.GlobalConfiguration;
import ai.dataeng.sqml.config.SqrlSettings;
import ai.dataeng.sqml.config.engines.FlinkConfiguration;
import ai.dataeng.sqml.config.engines.JDBCConfiguration;
import ai.dataeng.sqml.io.sources.dataset.DatasetRegistry;
import ai.dataeng.sqml.io.sources.dataset.SourceDataset;
import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import ai.dataeng.sqml.io.sources.impl.file.FileSourceConfiguration;
import ai.dataeng.sqml.io.sources.stats.SourceTableStatistics;
import ai.dataeng.sqml.planner.Dataset;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;

public class ConfigurationTest {

    static Path resourceDir = Paths.get("src","test","resources");
    static Path configYml = resourceDir.resolve("simple-config.yml");

    static final Path dbPath = Path.of("tmp");
    static final String jdbcURL = "jdbc:h2:"+dbPath.toAbsolutePath();

    public static final Path DATA_DIR = resourceDir.resolve("data");

    @BeforeEach
    public void deleteDatabase() throws IOException {
        FileUtils.cleanDirectory(ConfigurationTest.dbPath.toFile());
    }

    @Test
    public void testConfigFromFile() {
        GlobalConfiguration config = GlobalConfiguration.fromFile(configYml);
        validateConfig(config);
        assertEquals(config.getEngines().getJdbc().getDbURL(),"jdbc:h2:tmp/output");
        assertNotNull(config.getEngines().getFlink());
        assertEquals(config.getEnvironment().getMetastore().getDatabase(),"system");
    }

    @Test
    public void testSettings() {
        SqrlSettings settings = getDefaultSettings();
        Environment env = Environment.create(settings);
        assertNotNull(env.getDatasetRegistry());
    }

    @Test
    public void testDatasetRegistry() throws InterruptedException {
        SqrlSettings settings = getDefaultSettings(false);
        Environment env = Environment.create(settings);
        DatasetRegistry registry = env.getDatasetRegistry();
        assertNotNull(registry);

        String dsName = "bookclub";

        FileSourceConfiguration fileConfig = FileSourceConfiguration.builder()
                .path(DATA_DIR.toAbsolutePath().toString())
                .name(dsName)
                .build();

        assertTrue(fileConfig.validate(new ProcessMessage.ProcessBundle<>()));

        registry.addSource(fileConfig);
        SourceDataset ds = registry.getDataset(dsName);
        assertNotNull(ds);
        Set<String> tablenames = ds.getTables().stream().map(SourceTable::getName)
                .map(Name::getCanonical).collect(Collectors.toSet());
        assertEquals(ImmutableSet.of("book","person"), tablenames);
        assertTrue(ds.containsTable("person"));
        assertNotNull(ds.getTable("book"));
        assertNotNull(ds.getDigest().getCanonicalizer());
        assertEquals(dsName,ds.getDigest().getName().getCanonical());

        env.close();

        //Test that registry correctly persisted tables
        env = Environment.create(settings);
        registry = env.getDatasetRegistry();

        ds = registry.getDataset(dsName);
        assertNotNull(ds);
        tablenames = ds.getTables().stream().map(SourceTable::getName)
                .map(Name::getCanonical).collect(Collectors.toSet());
        assertEquals(ImmutableSet.of("book","person"), tablenames);

    }

    @Test
    public void testDatasetMonitoring() throws InterruptedException {
        SqrlSettings settings = getDefaultSettings(true);
        Environment env = Environment.create(settings);
        DatasetRegistry registry = env.getDatasetRegistry();

        String dsName = "bookclub";

        FileSourceConfiguration fileConfig = FileSourceConfiguration.builder()
                .path(DATA_DIR.toAbsolutePath().toString())
                .name(dsName)
                .build();

        registry.addSource(fileConfig);

        //Needs some time to wait for the flink pipeline to compile data

        SourceDataset ds = registry.getDataset(dsName);
        SourceTable book = ds.getTable("book");
        SourceTable person = ds.getTable("person");

        SourceTableStatistics stats = book.getStatistics();
        assertNotNull(stats);
        assertEquals(4,stats.getCount());
        assertEquals(5, person.getStatistics().getCount());
    }

    public static void validateConfig(GlobalConfiguration config) {
        ProcessMessage.ProcessBundle<ConfigurationError> errors = config.validate();
        for (ConfigurationError error : errors) {
            System.err.println(error);
        }
        if (errors.isFatal()) throw new IllegalArgumentException("Encountered fatal configuration errors");
    }

    public static SqrlSettings getDefaultSettings() {
        return getDefaultSettings(true);
    }

    public static SqrlSettings getDefaultSettings(boolean monitorSources) {
        GlobalConfiguration config = GlobalConfiguration.builder()
                .engines(GlobalConfiguration.Engines.builder()
                        .jdbc(JDBCConfiguration.builder()
                                .dbURL(jdbcURL)
                                .driverName("org.h2.Driver")
                                .dialect(JDBCConfiguration.Dialect.H2)
                                .build())
                        .flink(new FlinkConfiguration())
                        .build())
                .environment(EnvironmentConfiguration.builder()
                        .monitorSources(monitorSources)
                        .build())
                .build();
        validateConfig(config);
        return SqrlSettings.fromConfiguration(config);
    }

}
