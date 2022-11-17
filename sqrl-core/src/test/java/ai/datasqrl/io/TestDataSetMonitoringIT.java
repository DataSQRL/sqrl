package ai.datasqrl.io;

import ai.datasqrl.AbstractEngineIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.compile.DataDiscovery;
import ai.datasqrl.compile.loaders.DataSourceLoader;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.sources.DataSystemConfig;
import ai.datasqrl.io.sources.dataset.TableInput;
import ai.datasqrl.io.sources.dataset.TableSource;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import ai.datasqrl.schema.input.external.SchemaDefinition;
import ai.datasqrl.schema.input.external.SchemaExport;
import ai.datasqrl.util.FileTestUtil;
import ai.datasqrl.util.SnapshotTest;
import ai.datasqrl.util.TestDataset;
import ai.datasqrl.util.data.Retail;
import ai.datasqrl.util.junit.ArgumentProvider;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class TestDataSetMonitoringIT extends AbstractEngineIT {

    @SneakyThrows
    @ParameterizedTest
    @ArgumentsSource(TestDatasetPlusStreamEngine.class)
    public void testDatasetMonitoring(TestDataset example, IntegrationTestSettings.EnginePair engine) {
        initialize(IntegrationTestSettings.builder().stream(engine.getStream()).database(engine.getDatabase()).build());
        SnapshotTest.Snapshot snapshot = SnapshotTest.Snapshot.of(getClass(), example.getName(), engine.getName());

        List<TableSource> tables = discoverSchema(example);
        assertEquals(example.getNumTables(),tables.size());
        assertEquals(example.getTables(),tables.stream().map(TableSource::getName).map(Name::getCanonical).collect(Collectors.toSet()));

        //Write out table configurations
        for (TableSource table : tables) {
            String json = FileTestUtil.writeJson(table.getConfiguration());
            assertTrue(json.length()>0);
        }

        Name datasetName = tables.get(0).getPath().parent().getLast();
        FlexibleDatasetSchema combinedSchema = DataDiscovery.combineSchema(tables);

        //Write out combined schema file
        SchemaExport export = new SchemaExport();
        SchemaDefinition outputSchema = export.export(Map.of(datasetName, combinedSchema));
        snapshot.addContent(FileTestUtil.writeYaml(outputSchema),"combined schema");
        snapshot.createOrValidate();
    }

    private List<TableSource> discoverSchema(TestDataset example) {
        DataDiscovery discovery = new DataDiscovery(sqrlSettings);
        ErrorCollector errors = ErrorCollector.root();
        DataSystemConfig systemConfig = getSystemConfigBuilder(example).build();
        List<TableInput> inputTables = discovery.discoverTables(systemConfig, errors);
        assertFalse(errors.isFatal(), errors.toString());
        assertEquals(example.getNumTables(), inputTables.size());
        discovery.monitorTables(inputTables, errors);
        assertFalse(errors.isFatal(), errors.toString());
        List<TableSource> sourceTables = discovery.discoverSchema(inputTables, errors);
        assertFalse(errors.isFatal(), errors.toString());
        assertEquals(example.getNumTables(), sourceTables.size());
        return sourceTables;
    }

    static class TestDatasetPlusStreamEngine implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            List<IntegrationTestSettings.EnginePair> engines = List.of(
                    new IntegrationTestSettings.EnginePair(IntegrationTestSettings.DatabaseEngine.INMEMORY, IntegrationTestSettings.StreamEngine.INMEMORY)
                    ,new IntegrationTestSettings.EnginePair(IntegrationTestSettings.DatabaseEngine.POSTGRES, IntegrationTestSettings.StreamEngine.FLINK)
            );
            return ArgumentProvider.crossProduct(TestDataset.getAll(), engines);
        }
    }

    /**
     * This method is only used to generate schemas for testing purposes and is not a test itself
     */
    @Test
    @Disabled
    public void generateSchema() {
        generateTableConfigAndSchemaInDataDir(Retail.INSTANCE);
    }

    @SneakyThrows
    public void generateTableConfigAndSchemaInDataDir(TestDataset example) {
        assertTrue(example.getNumTables()>0);
        initialize(IntegrationTestSettings.getInMemory());
        List<TableSource> tables = discoverSchema(example);

        Path destinationDir = example.getRootPackageDirectory().resolve(example.getName());
        //Write out table configurations
        for (TableSource table : tables) {
            Path tableConfigFile = destinationDir.resolve(table.getName().getCanonical()+DataSourceLoader.TABLE_FILE_SUFFIX);
            FileTestUtil.writeJson(tableConfigFile,table.getConfiguration());
        }

        Name datasetName = tables.get(0).getPath().parent().getLast();
        FlexibleDatasetSchema combinedSchema = DataDiscovery.combineSchema(tables);

        //Write out combined schema file
        SchemaExport export = new SchemaExport();
        SchemaDefinition outputSchema = export.export(Map.of(datasetName, combinedSchema));
        Path schemaFile = destinationDir.resolve(DataSourceLoader.PACKAGE_SCHEMA_FILE);
        FileTestUtil.writeYaml(schemaFile,outputSchema);
    }

}
