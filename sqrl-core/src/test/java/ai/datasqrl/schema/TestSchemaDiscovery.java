package ai.datasqrl.schema;

import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import ai.datasqrl.util.TestDataset;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestSchemaDiscovery extends AbstractSchemaIT {

    @ParameterizedTest
    @ArgumentsSource(TestDatasetPlusStreamEngine.class)
    public void testDatasetMonitoring(TestDataset example, IntegrationTestSettings.EnginePair engine) {
        initialize(IntegrationTestSettings.builder().monitorSources(true).stream(engine.getStream()).database(engine.getDatabase()).build());

        registerDataset(example);
        FlexibleDatasetSchema definedSchema = getDiscoveredSchema(example);

        ImportManager imports = new ImportManager(sourceRegistry);
        ErrorCollector schemaErrs = ErrorCollector.root();

        for (FlexibleDatasetSchema.TableField definedTable : definedSchema) {
            ImportManager.SourceTableImport impTable = (ImportManager.SourceTableImport) imports.importTable(Name.system(example.getName()),definedTable.getName(),
                    SchemaAdjustmentSettings.DEFAULT,schemaErrs);
            assertNotNull(impTable);
            FlexibleDatasetSchema.TableField discoveredTable = impTable.getSourceSchema();
            assertNotNull(discoveredTable);
            assertEquals(definedTable,discoveredTable);
        }
    }

    static class TestDatasetPlusStreamEngine implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return TestDataset.generateAsArguments(td -> td.getDiscoveredSchema().isPresent(), List.of(
                    new IntegrationTestSettings.EnginePair(IntegrationTestSettings.DatabaseEngine.INMEMORY, IntegrationTestSettings.StreamEngine.INMEMORY)
            ));
        }
    }

}
