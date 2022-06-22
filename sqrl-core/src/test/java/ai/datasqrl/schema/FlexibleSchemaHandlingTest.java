package ai.datasqrl.schema;

import ai.datasqrl.AbstractSQRLIntegrationTest;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.SqrlScript;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.constraint.Constraint;
import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import ai.datasqrl.schema.input.external.SchemaDefinition;
import ai.datasqrl.schema.input.external.SchemaImport;
import ai.datasqrl.util.TestDataset;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class FlexibleSchemaHandlingTest extends AbstractSQRLIntegrationTest {

    @SneakyThrows
    public FlexibleDatasetSchema getSchema(TestDataset example) {
        ErrorCollector errors = ErrorCollector.root();
        sourceRegistry.addOrUpdateSource(example.getName(), example.getSource(), errors);
        assertFalse(errors.isFatal(), errors.toString());

        SchemaDefinition schemaDef = SqrlScript.Config.parseSchema(example.getInputSchema().get());
        SchemaImport importer = new SchemaImport(sourceRegistry, Constraint.FACTORY_LOOKUP);
        errors = ErrorCollector.root();
        Map<Name, FlexibleDatasetSchema> schema = importer.convertImportSchema(schemaDef, errors);

        assertFalse(errors.isFatal(),errors.toString());
        assertFalse(schema.isEmpty());
        assertTrue(schema.size()==1);
        FlexibleDatasetSchema dsSchema = schema.get(Name.system(example.getName()));
        assertNotNull(dsSchema);
        return dsSchema;
    }

    @BeforeEach
    public void setup() {
        initialize(IntegrationTestSettings.getInMemory(false));

    }

    @ParameterizedTest
    @ArgumentsSource(TestDataset.WithSchemaProvider.class)
    public void simpleTest(TestDataset example) {
        FlexibleDatasetSchema schema = getSchema(example);
    }





}
