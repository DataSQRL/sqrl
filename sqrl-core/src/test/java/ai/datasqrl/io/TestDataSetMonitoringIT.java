package ai.datasqrl.io;

import ai.datasqrl.AbstractEngineIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.compile.DataDiscovery;
import ai.datasqrl.compile.loaders.DataSourceLoader;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.sources.DataSystemConfig;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import ai.datasqrl.schema.input.external.SchemaDefinition;
import ai.datasqrl.schema.input.external.SchemaExport;
import ai.datasqrl.util.SnapshotTest;
import ai.datasqrl.util.TestDataset;
import ai.datasqrl.util.data.Retail;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
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
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
public class TestDataSetMonitoringIT extends AbstractEngineIT {

    @SneakyThrows
    @ParameterizedTest
    @ArgumentsSource(TestDatasetPlusStreamEngine.class)
    public void testDatasetMonitoring(TestDataset example, IntegrationTestSettings.EnginePair engine) {
        initialize(IntegrationTestSettings.builder().stream(engine.getStream()).database(engine.getDatabase()).build());
        SnapshotTest.Snapshot snapshot = SnapshotTest.Snapshot.of(getClass(), example.getName(), engine.getName());

        SchemaDefinition outputSchema = discoverSchema(example);
        YAMLMapper mapper = new YAMLMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        snapshot.addContent(mapper.writeValueAsString(outputSchema));
        snapshot.createOrValidate();
    }

    private SchemaDefinition discoverSchema(TestDataset example) {
        DataDiscovery discovery = new DataDiscovery(sqrlSettings);
        ErrorCollector errors = ErrorCollector.root();
        DataSystemConfig systemConfig = getSystemConfigBuilder(example).build();
        Optional<NamePath> path = discovery.monitorTables(systemConfig,errors);
        assertTrue(path.isPresent());
        assertFalse(errors.isFatal(), errors.toString());
        FlexibleDatasetSchema schema = discovery.discoverSchema(path.get(),errors);
        assertFalse(errors.isFatal(), errors.toString());

        SchemaExport export = new SchemaExport();
        SchemaDefinition outputSchema = export.export(Map.of(path.get().getLast(), schema));
        return outputSchema;
    }

    static class TestDatasetPlusStreamEngine implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return TestDataset.generateAsArguments(List.of(
                    new IntegrationTestSettings.EnginePair(IntegrationTestSettings.DatabaseEngine.INMEMORY, IntegrationTestSettings.StreamEngine.INMEMORY)
                    ,new IntegrationTestSettings.EnginePair(IntegrationTestSettings.DatabaseEngine.POSTGRES, IntegrationTestSettings.StreamEngine.FLINK)
            ));
        }
    }

    /**
     * This method is only used to generate schemas for testing purposes and is not a test itself
     */
    @Test
    public void generateSchema() {
        TestDataset example = Retail.INSTANCE;
        generateSchema(example,example.getDataDirectory().resolve(DataSourceLoader.PACKAGE_SCHEMA_FILE));
    }

    @SneakyThrows
    public void generateSchema(TestDataset example, Path schemaFile) {
        initialize(IntegrationTestSettings.getInMemory());
        SchemaDefinition outputSchema = discoverSchema(example);
        YAMLMapper mapper = new YAMLMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.writeValue(schemaFile.toFile(),outputSchema);
    }

}
