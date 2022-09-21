package ai.datasqrl.schema;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.SqrlScript;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.constraint.Constraint;
import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import ai.datasqrl.schema.input.external.SchemaDefinition;
import ai.datasqrl.schema.input.external.SchemaImport;
import ai.datasqrl.util.TestDataset;
import lombok.SneakyThrows;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class AbstractSchemaIT extends AbstractSQRLIT {

    public void registerDataset(TestDataset example) {
        ErrorCollector errors = ErrorCollector.root();
        sourceRegistry.addOrUpdateSource(example.getName(), example.getSource(), errors);
        assertFalse(errors.isFatal(), errors.toString());
    }

    public FlexibleDatasetSchema getPreSchema(TestDataset example) {
        return getSchema(example.getName(),example.getInputSchema());
    }

    public FlexibleDatasetSchema getDiscoveredSchema(TestDataset example) {
        return getSchema(example.getName(),example.getDiscoveredSchema());
    }

    @SneakyThrows
    public FlexibleDatasetSchema getSchema(String datasetName, Optional<String> schemaString) {
        assertTrue(schemaString.isPresent());
        SchemaDefinition schemaDef = SqrlScript.Config.parseSchema(schemaString.get());
        SchemaImport importer = new SchemaImport(sourceRegistry, Constraint.FACTORY_LOOKUP);
        ErrorCollector errors = ErrorCollector.root();
        Map<Name, FlexibleDatasetSchema> schema = importer.convertImportSchema(schemaDef, errors);

        assertFalse(errors.isFatal(),errors.toString());
        assertFalse(schema.isEmpty());
        assertTrue(schema.size()==1);
        FlexibleDatasetSchema dsSchema = schema.get(Name.system(datasetName));
        assertNotNull(dsSchema);
        return dsSchema;
    }

}
