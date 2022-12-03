package com.datasqrl.io;

import com.datasqrl.AbstractEngineIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.compile.DataDiscovery;
import com.datasqrl.compile.loaders.DataSource;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableInput;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.name.Name;
import com.datasqrl.schema.input.FlexibleDatasetSchema;
import com.datasqrl.schema.input.external.SchemaDefinition;
import com.datasqrl.schema.input.external.SchemaExport;
import com.datasqrl.util.FileTestUtil;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestDataset;
import com.datasqrl.util.data.Retail;
import com.datasqrl.util.junit.ArgumentProvider;
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
        ErrorCollector errors = ErrorCollector.root();
        DataDiscovery discovery = new DataDiscovery(errors, engineSettings);
        DataSystemConfig systemConfig = getSystemConfigBuilder(example).build();
        List<TableInput> inputTables = discovery.discoverTables(systemConfig);
        assertFalse(errors.isFatal(), errors.toString());
        assertEquals(example.getNumTables(), inputTables.size());
        discovery.monitorTables(inputTables);
        assertFalse(errors.isFatal(), errors.toString());
        List<TableSource> sourceTables = discovery.discoverSchema(inputTables);
        assertFalse(errors.isFatal(), errors.toString());
        assertEquals(example.getNumTables(), sourceTables.size());
        return sourceTables;
    }

    static class TestDatasetPlusStreamEngine implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            List<IntegrationTestSettings.EnginePair> engines = List.of(
                    new IntegrationTestSettings.EnginePair(IntegrationTestSettings.DatabaseEngine.INMEMORY, IntegrationTestSettings.StreamEngine.INMEMORY),
                    new IntegrationTestSettings.EnginePair(IntegrationTestSettings.DatabaseEngine.POSTGRES, IntegrationTestSettings.StreamEngine.FLINK)
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
            Path tableConfigFile = destinationDir.resolve(table.getName().getCanonical()+ DataSource.TABLE_FILE_SUFFIX);
            FileTestUtil.writeJson(tableConfigFile,table.getConfiguration());
        }

        Name datasetName = tables.get(0).getPath().parent().getLast();
        FlexibleDatasetSchema combinedSchema = DataDiscovery.combineSchema(tables);

        //Write out combined schema file
        SchemaExport export = new SchemaExport();
        SchemaDefinition outputSchema = export.export(Map.of(datasetName, combinedSchema));
        Path schemaFile = destinationDir.resolve(DataSource.PACKAGE_SCHEMA_FILE);
        FileTestUtil.writeYaml(schemaFile,outputSchema);
    }

}
