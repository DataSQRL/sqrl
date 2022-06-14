package ai.datasqrl.api;

import ai.datasqrl.environment.Environment;
import ai.datasqrl.TestDatabase;
import ai.datasqrl.config.EnvironmentConfiguration;
import ai.datasqrl.config.EnvironmentConfiguration.MetaData;
import ai.datasqrl.config.GlobalConfiguration;
import ai.datasqrl.config.SqrlSettings;
import ai.datasqrl.config.engines.FlinkConfiguration;
import ai.datasqrl.io.formats.FileFormat;
import ai.datasqrl.io.formats.FormatConfiguration;
import ai.datasqrl.io.formats.JsonLineFormat;
import ai.datasqrl.io.impl.file.DirectorySinkImplementation;
import ai.datasqrl.io.impl.file.DirectorySourceImplementation;
import ai.datasqrl.io.sinks.DataSink;
import ai.datasqrl.io.sinks.DataSinkConfiguration;
import ai.datasqrl.io.sinks.DataSinkRegistration;
import ai.datasqrl.io.sinks.registry.DataSinkRegistry;
import ai.datasqrl.io.sources.DataSourceConfiguration;
import ai.datasqrl.io.sources.DataSourceUpdate;
import ai.datasqrl.io.sources.SourceTableConfiguration;
import ai.datasqrl.io.sources.dataset.DatasetRegistry;
import ai.datasqrl.io.sources.dataset.SourceDataset;
import ai.datasqrl.io.sources.dataset.SourceTable;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.config.error.ErrorCollector;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;

public class ConfigurationTest {
    public static TestDatabase testDatabase = new TestDatabase();

    public static Path resourceDir = Paths.get("src","test","resources");
    public static Path configYml = resourceDir.resolve("simple-config.yml");

    public static final Path dbPath = Path.of("tmp");

    public static final Path DATA_DIR = resourceDir.resolve("data");

    public static final Path[] BOOK_FILES = new Path[]{DATA_DIR.resolve("book_001.json"), DATA_DIR.resolve("book_002.json")};

    private Environment env = null;

    @BeforeEach
    public void deleteDatabase() throws IOException {
        FileUtils.cleanDirectory(ConfigurationTest.dbPath.toFile());
    }

    @AfterEach
    public void close() {
        if (env!=null) env.close();
    }

    @Test
    public void testConfigFromFile() {
        GlobalConfiguration config = GlobalConfiguration.fromFile(configYml);
        validateConfig(config);
        assertEquals(config.getEngines().getJdbc().getDbURL(),"jdbc:h2:tmp/output");
        assertNotNull(config.getEngines().getFlink());
        assertEquals(config.getEnvironment().getMetastore().getDatabase(),"system");
        assertEquals(1, config.getSources().size());
        assertTrue(config.getSources().get(0).getSource() instanceof DirectorySourceImplementation);
        assertEquals(1, config.getSinks().size());
        assertTrue(config.getSinks().get(0).getSink() instanceof DirectorySinkImplementation);
        assertTrue(config.getSinks().get(0).getConfig().getFormat() instanceof JsonLineFormat.Configuration);
        assertEquals("local",config.getSinks().get(0).getName());
    }

    @Test
    public void testSettings() {
        SqrlSettings settings = getDefaultSettings();
        env = Environment.create(settings);
        assertNotNull(env.getDatasetRegistry());
    }

    @Test
    public void testDatasetRegistry() throws InterruptedException {
        SqrlSettings settings = getDefaultSettings(false);
        env = Environment.create(settings);
        DatasetRegistry registry = env.getDatasetRegistry();
        assertNotNull(registry);

        //Directory
        String dsName = "bookclub";
        DirectorySourceImplementation fileConfig = DirectorySourceImplementation.builder()
                .uri(DATA_DIR.toAbsolutePath().toString())
                .build();



        ErrorCollector errors = ErrorCollector.root();
        registry.addOrUpdateSource(dsName, fileConfig, errors);
        assertFalse(errors.isFatal());
        SourceDataset ds = registry.getDataset(dsName);
        assertNotNull(ds);
        Set<String> tablenames = ds.getTables().stream().map(SourceTable::getName)
                .map(Name::getCanonical).collect(Collectors.toSet());
        assertEquals(ImmutableSet.of("book","person"), tablenames);
        assertNotNull(ds.getDigest().getCanonicalizer());
        Assertions.assertEquals(dsName,ds.getDigest().getName().getCanonical());
        assertTrue(ds.getSource().getImplementation() instanceof DirectorySourceImplementation);
        assertNull(ds.getSource().getConfig().getFormat());

        assertTrue(ds.containsTable("person"));
        SourceTable table = ds.getTable("book");
        assertNotNull(table);
        Assertions.assertEquals("book",table.getName().getCanonical());
        assertEquals(ds,table.getDataset());
        assertEquals("bookclub.book",table.qualifiedName());
        assertEquals("book", table.getConfiguration().getIdentifier());
        FormatConfiguration format = table.getConfiguration().getFormat();
        assertNotNull(format);
        assertEquals(FileFormat.JSON,format.getFileFormat());

        //Without table discovery
        String ds2Name = "anotherbook";
        DataSourceUpdate update = DataSourceUpdate.builder().name(ds2Name).source(fileConfig)
                .config(DataSourceConfiguration.builder().format(new JsonLineFormat.Configuration()).build())
                .tables(ImmutableList.of(SourceTableConfiguration.builder().name("test").identifier("book").build()))
                .discoverTables(false)
                .build();


        registry.addOrUpdateSource(update,errors);
        assertFalse(errors.isFatal());
        SourceDataset ds2 = registry.getDataset(ds2Name);
        assertEquals(1,ds2.getTables().size());
        assertNotNull(ds2.getTable("test"));
        assertEquals(FileFormat.JSON,ds2.getTable("test").getConfiguration().getFileFormat());

        env.close();

        //Test that registry correctly persisted tables
        env = Environment.create(settings);
        registry = env.getDatasetRegistry();

        ds = registry.getDataset(dsName);
        assertNotNull(ds);
        tablenames = ds.getTables().stream().map(SourceTable::getName)
                .map(Name::getCanonical).collect(Collectors.toSet());
        assertEquals(ImmutableSet.of("book","person"), tablenames);
        assertEquals(FileFormat.JSON,ds.getTable("book").getConfiguration().getFileFormat());

        //Test deletions
        assertNotNull(registry.getDataset(ds2Name));
        ds2 = registry.getDataset(ds2Name);
        assertEquals(1,ds2.getTables().size());
        ds2.removeTable("test");
        assertEquals(0,ds2.getTables().size());

        registry.removeSource(ds2Name);
        assertNull(registry.getDataset(ds2Name));
        assertEquals(1,registry.getDatasets().size());

        env.close();
        env = Environment.create(settings);
        registry = env.getDatasetRegistry();
        assertEquals(1,registry.getDatasets().size());

    }

    @Test
    public void testDataSink() {
        SqrlSettings settings = getDefaultSettings(false);
        env = Environment.create(settings);
        DataSinkRegistry registry = env.getDataSinkRegistry();
        assertNotNull(registry);

        DirectorySinkImplementation fileSink = DirectorySinkImplementation.builder().uri(DATA_DIR.toAbsolutePath().toString()).build();
        DataSinkRegistration reg = DataSinkRegistration.builder().name("sink")
                .sink(fileSink)
                .config(DataSinkConfiguration.builder().format(new JsonLineFormat.Configuration()).build())
                .build();

        ErrorCollector errors = ErrorCollector.root();
        registry.addOrUpdateSink(reg,errors);
        assertFalse(errors.isFatal());

        DataSink sink = registry.getSink("sink");
        assertEquals(FileFormat.JSON,sink.getRegistration().getConfig().getFormat().getFileFormat());
    }

    @Test
    public void testDatasetMonitoring() throws InterruptedException {
        SqrlSettings settings = getDefaultSettings(true);
        env = Environment.create(settings);
        DatasetRegistry registry = env.getDatasetRegistry();

        String dsName = "bookclub";
        DirectorySourceImplementation fileConfig = DirectorySourceImplementation.builder()
                .uri(DATA_DIR.toAbsolutePath().toString())
                .build();

        ErrorCollector errors = ErrorCollector.root();
        registry.addOrUpdateSource(dsName, fileConfig, errors);
        if (errors.isFatal()) System.out.println(errors);
        assertFalse(errors.isFatal());

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
        ErrorCollector errors = config.validate();

        if (errors.hasErrors()) {
            System.err.println(errors.toString());
        }
        if (errors.isFatal()) throw new IllegalArgumentException("Encountered fatal configuration errors");
    }

    public static SqrlSettings getDefaultSettings() {
        return getDefaultSettings(true);
    }

    public static SqrlSettings getDefaultSettings(boolean monitorSources) {
        GlobalConfiguration config = GlobalConfiguration.builder()
                .engines(GlobalConfiguration.Engines.builder()
                        .jdbc(testDatabase.getJdbcConfiguration())
                        .flink(new FlinkConfiguration())
                        .build())
                .environment(EnvironmentConfiguration.builder()
                        .monitorSources(monitorSources)
                        .metastore(MetaData.builder()
                            .database(MetaData.DEFAULT_DATABASE)
                            .build())
                        .build())
                .build();
        validateConfig(config);
        return SqrlSettings.fromConfiguration(config);
    }
}
