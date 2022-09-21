package ai.datasqrl.api;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.environment.Environment;
import ai.datasqrl.config.GlobalConfiguration;
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
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.util.TestDataset;
import ai.datasqrl.util.TestResources;
import ai.datasqrl.util.data.BookClub;
import ai.datasqrl.util.data.C360;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ConfigurationIT extends AbstractSQRLIT {

    @Test
    public void testConfigFromFile() {
        GlobalConfiguration config = GlobalConfiguration.fromFile(TestResources.CONFIG_YML);
        IntegrationTestSettings.validateConfig(config);
        assertEquals(config.getEngines().getJdbc().getDbURL(),"jdbc:h2:tmp/output");
        assertNotNull(config.getEngines().getFlink());
        assertEquals(config.getEnvironment().getMetastore().getDatabaseName(),"system");
        assertEquals(1, config.getSources().size());
        assertTrue(config.getSources().get(0).getSource() instanceof DirectorySourceImplementation);
        assertEquals(1, config.getSinks().size());
        assertTrue(config.getSinks().get(0).getSink() instanceof DirectorySinkImplementation);
        assertTrue(config.getSinks().get(0).getConfig().getFormat() instanceof JsonLineFormat.Configuration);
        assertEquals("local",config.getSinks().get(0).getName());
    }

    @Test
    public void testSettings() {
        initialize(IntegrationTestSettings.getInMemory());
        assertNotNull(env.getDatasetRegistry());
    }

    @Test
    public void testDatasetRegistryBookClub() {
        testDatasetRegistry(BookClub.INSTANCE);
    }

    @Test
    public void testDatasetRegistryC360() {
        testDatasetRegistry(C360.BASIC);
    }

    public void testDatasetRegistry(TestDataset example) {
        initialize(IntegrationTestSettings.getInMemory(false));
        DatasetRegistry registry = env.getDatasetRegistry();

        assertNotNull(registry);

        example.registerSource(env);
        SourceDataset ds = registry.getDataset(example.getName());
        assertNotNull(ds);
        Set<String> tablenames = ds.getTables().stream().map(SourceTable::getName)
                .map(Name::getCanonical).collect(Collectors.toSet());
        Map<String,Integer> tblCounts = example.getTableCounts();
        assertEquals(tblCounts.keySet(), tablenames);
        assertNotNull(ds.getDigest().getCanonicalizer());
        Assertions.assertEquals(example.getName(),ds.getDigest().getName().getCanonical());
        assertTrue(ds.getSource().getImplementation() instanceof DirectorySourceImplementation);
        assertNull(ds.getSource().getConfig().getFormat());

        for (String tblname : tblCounts.keySet()) {
            assertTrue(ds.containsTable(tblname));
            SourceTable table = ds.getTable(tblname);
            assertNotNull(table);
            Assertions.assertEquals(tblname,table.getName().getCanonical());
            assertEquals(ds,table.getDataset());
            assertEquals(example.getName() + "." + tblname,table.qualifiedName());
            assertEquals(tblname, table.getConfiguration().getIdentifier());
            FormatConfiguration format = table.getConfiguration().getFormat();
            assertNotNull(format);
        }

        env.close();

        //Test that registry correctly persisted tables
        env = Environment.create(sqrlSettings);
        registry = env.getDatasetRegistry();

        ds = registry.getDataset(example.getName());
        assertNotNull(ds);
        tablenames = ds.getTables().stream().map(SourceTable::getName)
                .map(Name::getCanonical).collect(Collectors.toSet());
        assertEquals(tblCounts.keySet(), tablenames);

        //Test deletions
        assertEquals(tblCounts.size(),ds.getTables().size());
        ds.removeTable(tblCounts.keySet().iterator().next());
        assertEquals(tblCounts.size()-1,ds.getTables().size());

        registry.removeSource(example.getName());
        assertNull(registry.getDataset(example.getName()));
        assertEquals(0,registry.getDatasets().size());

        env.close();
        env = Environment.create(sqrlSettings);
        registry = env.getDatasetRegistry();
        assertEquals(0,registry.getDatasets().size());

    }

    @Test
    public void testDatasetRegistryWithExplicitTableRegistration() {
        initialize(IntegrationTestSettings.getInMemory(false));
        DatasetRegistry registry = env.getDatasetRegistry();
        TestDataset example = BookClub.INSTANCE;

        //Without table discovery
        String ds2Name = "explicitDS";
        DataSourceUpdate update = DataSourceUpdate.builder().name(ds2Name).source(example.getSource())
                .config(DataSourceConfiguration.builder().format(new JsonLineFormat.Configuration()).build())
                .tables(ImmutableList.of(SourceTableConfiguration.builder().name("test").identifier("book").build()))
                .discoverTables(false)
                .build();
        ErrorCollector errors = ErrorCollector.root();

        registry.addOrUpdateSource(update,errors);
        assertFalse(errors.isFatal(),errors.toString());
        SourceDataset ds2 = registry.getDataset(ds2Name);
        assertEquals(1,ds2.getTables().size());
        assertNotNull(ds2.getTable("test"));
        assertEquals(FileFormat.JSON,ds2.getTable("test").getConfiguration().getFileFormat());
    }

    @Test
    public void testDataSink() {
        initialize(IntegrationTestSettings.getInMemory(false));

        DataSinkRegistry registry = env.getDataSinkRegistry();
        assertNotNull(registry);

        DirectorySinkImplementation fileSink = DirectorySinkImplementation.builder().uri(BookClub.DATA_DIR.toAbsolutePath().toString()).build();
        DataSinkRegistration reg = DataSinkRegistration.builder().name("sink")
                .sink(fileSink)
                .config(DataSinkConfiguration.builder().format(new JsonLineFormat.Configuration()).build())
                .build();

        ErrorCollector errors = ErrorCollector.root();
        registry.addOrUpdateSink(reg,errors);
        assertFalse(errors.isFatal(),errors.toString());

        DataSink sink = registry.getSink("sink");
        assertEquals(FileFormat.JSON,sink.getRegistration().getConfig().getFormat().getFileFormat());
    }


}
