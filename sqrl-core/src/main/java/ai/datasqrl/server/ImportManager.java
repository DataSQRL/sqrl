package ai.datasqrl.server;

import ai.datasqrl.io.sources.dataset.DatasetRegistry;
import ai.datasqrl.io.sources.dataset.SourceDataset;
import ai.datasqrl.io.sources.dataset.SourceTable;
import ai.datasqrl.io.sources.stats.SchemaGenerator;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NameCanonicalizer;
import ai.datasqrl.schema.type.RelationType;
import ai.datasqrl.schema.type.StandardField;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.schema.type.constraint.Constraint;
import ai.datasqrl.schema.type.schema.FlexibleDatasetSchema;
import ai.datasqrl.schema.type.schema.external.SchemaDefinition;
import ai.datasqrl.schema.type.schema.external.SchemaImport;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.NonNull;
import lombok.Value;

public class ImportManager {

    public static final NameCanonicalizer IMPORT_CANONICALIZER = NameCanonicalizer.SYSTEM;

    private final DatasetRegistry datasetRegistry;
    private Map<Name, FlexibleDatasetSchema> userSchema = Collections.EMPTY_MAP;
    private Map<Name, RelationType<StandardField>> scriptSchemas = new HashMap<>();

    public ImportManager(DatasetRegistry datasetRegistry) {
        this.datasetRegistry = datasetRegistry;
    }

    public DatasetRegistry getDatasetRegistry() {
        return datasetRegistry;
    }

    public ErrorCollector registerUserSchema(SchemaDefinition yamlSchema) {
        ErrorCollector errors = ErrorCollector.root();
        SchemaImport importer = new SchemaImport(datasetRegistry, Constraint.FACTORY_LOOKUP);
        Map<Name, FlexibleDatasetSchema> result = importer.convertImportSchema(yamlSchema, errors);
        if (!errors.isFatal()) {
            registerUserSchema(result);
        }
        return errors;
    }

    public void registerUserSchema(Map<Name, FlexibleDatasetSchema> schema) {
        Preconditions.checkArgument(schema!=null && !schema.isEmpty(), "Schema is empty");
        this.userSchema = schema;
    }

    public void registerScript(@NonNull Name name, @NonNull RelationType<StandardField> datasetSchema) {
        Preconditions.checkArgument(scriptSchemas.containsKey(name),"Duplicate name for script import: %s", name);
        scriptSchemas.put(name,datasetSchema);
    }

    public List<TableImport> importAllTables(@NonNull Name datasetName,
                                             ErrorCollector errors) {
        List<TableImport> imports = new ArrayList<>();
        if (scriptSchemas.containsKey(datasetName)) {
            throw new UnsupportedOperationException("Not yet implemented");
        } else {
            SourceDataset dsDef = datasetRegistry.getDataset(datasetName);
            if (dsDef == null) {
                errors.fatal( "Unknown dataset: %s", datasetName);
                return imports; //abort
            }
            for (SourceTable table : dsDef.getTables()) {
                TableImport tblimport = importTable(datasetName, table.getName(),errors);
                if (tblimport!=null) imports.add(tblimport);
            }
        }
        return imports;
    }

    public SourceTableImport importTable(@NonNull Name datasetName,  @NonNull Name tableName,
                                   ErrorCollector errors) {
        if (scriptSchemas.containsKey(datasetName)) {
            throw new UnsupportedOperationException("Not yet implemented");
        } else {
            SourceDataset dsDef = datasetRegistry.getDataset(datasetName);
            if (dsDef == null) {
                errors.fatal("Unknown dataset: %s", datasetName);
                return null; //abort
            }
            errors = errors.resolve(datasetName);
            SourceTable table = dsDef.getTable(tableName);
            if (table == null) {
                errors.fatal("Unknown table: %s", tableName);
                return null;
            }
            FlexibleDatasetSchema userDSSchema = userSchema.get(datasetName);
            if (userDSSchema == null) userDSSchema = FlexibleDatasetSchema.EMPTY;
            FlexibleDatasetSchema.TableField tbField = createTable(table, datasetName,
                    userDSSchema.getFieldByName(table.getName()),errors);
            System.out.println(errors);
            //schemaConverter.convert(tbField,imp.asName.orElse(table.getName()))
            return new SourceTableImport(tableName, table, tbField);
        }
    }

    public SourceTableImport resolveTable(@NonNull Name datasetName, @NonNull Name tableName,
        Optional<Name> alias, ErrorCollector errors) {
        SourceTableImport sourceTableImport = importTable(datasetName, tableName, errors);
        return sourceTableImport;
    }

    public interface TableImport {

        Name getTableName();

        boolean isSource();

        default boolean isScript() {
            return !isSource();
        }

    }

    @Value
    public static class SourceTableImport implements TableImport {

        @NonNull
        private final Name tableName;
        @NonNull
        private final SourceTable table;
        @NonNull
        private final FlexibleDatasetSchema.TableField sourceSchema;

        @Override
        public boolean isSource() {
            return true;
        }
    }

    @Value
    public static class ScriptTableImport implements TableImport {

        @NonNull
        private final Name tableName;

        @Override
        public boolean isSource() {
            return false;
        }
        //TODO: to be filled out
    }

    private FlexibleDatasetSchema.TableField createTable(SourceTable table, Name datasetname,
                                                         FlexibleDatasetSchema.TableField userSchema,
                                                         ErrorCollector errors) {
        SchemaGenerator generator = new SchemaGenerator();
        SourceTableStatistics stats = table.getStatistics();
        errors = errors.resolve(datasetname);
        if (userSchema==null) {
            if (stats.getCount()==0) {
                errors.fatal("We cannot infer schema for table [%s] due to lack of data. Need to provide user schema.", table.getName());
            }
            userSchema = FlexibleDatasetSchema.TableField.empty(table.getName());
        }
        FlexibleDatasetSchema.TableField result = generator.mergeSchema(stats, userSchema,
                errors.resolve(table.getName()));
        return result;
    }
}
