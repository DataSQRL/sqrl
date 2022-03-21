package ai.dataeng.sqml.planner.operator;

import ai.dataeng.sqml.io.sources.dataset.DatasetLookup;
import ai.dataeng.sqml.io.sources.dataset.SourceDataset;
import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import ai.dataeng.sqml.io.sources.stats.SchemaGenerator;
import ai.dataeng.sqml.io.sources.stats.SourceTableStatistics;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.tree.name.NamePath;
import ai.dataeng.sqml.type.RelationType;
import ai.dataeng.sqml.type.StandardField;
import ai.dataeng.sqml.type.basic.ProcessMessage.ProcessBundle;
import ai.dataeng.sqml.type.constraint.Constraint;
import ai.dataeng.sqml.type.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.type.schema.SchemaConversionError;
import ai.dataeng.sqml.type.schema.external.SchemaDefinition;
import ai.dataeng.sqml.type.schema.external.SchemaImport;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import lombok.Value;

public class ImportManager {

    public static final NameCanonicalizer IMPORT_CANONICALIZER = NameCanonicalizer.SYSTEM;

    private final DatasetLookup datasetLookup;
    private Map<Name, FlexibleDatasetSchema> userSchema = Collections.EMPTY_MAP;
    private Map<Name, RelationType<StandardField>> scriptSchemas = new HashMap<>();

    public ImportManager(DatasetLookup datasetLookup) {
        this.datasetLookup = datasetLookup;
    }

    public DatasetLookup getDatasetLookup() {
        return datasetLookup;
    }

    public ProcessBundle<SchemaConversionError> registerUserSchema(SchemaDefinition yamlSchema) {
        SchemaImport importer = new SchemaImport(datasetLookup, Constraint.FACTORY_LOOKUP);
        Map<Name, FlexibleDatasetSchema> result = importer.convertImportSchema(yamlSchema);
        if (!importer.getErrors().isFatal()) {
            registerUserSchema(result);
        }
        return importer.getErrors();
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
                                             ProcessBundle<SchemaConversionError> errors) {
        List<TableImport> imports = new ArrayList<>();
        if (scriptSchemas.containsKey(datasetName)) {
            throw new UnsupportedOperationException("Not yet implemented");
        } else {
            SourceDataset dsDef = datasetLookup.getDataset(datasetName);
            if (dsDef == null) {
                errors.add(SchemaConversionError.fatal(NamePath.ROOT, "Unknown dataset: %s", datasetName));
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
                                   ProcessBundle<SchemaConversionError> errors) {
        if (scriptSchemas.containsKey(datasetName)) {
            throw new UnsupportedOperationException("Not yet implemented");
        } else {
            SourceDataset dsDef = datasetLookup.getDataset(datasetName);
            if (dsDef == null) {
                errors.add(SchemaConversionError.fatal(NamePath.ROOT, "Unknown dataset: %s", datasetName));
                return null; //abort
            }
            SourceTable table = dsDef.getTable(tableName);
            if (table == null) {
                errors.add(SchemaConversionError.fatal(NamePath.of(datasetName), "Unknown table: %s", tableName));
                return null;
            }
            FlexibleDatasetSchema userDSSchema = userSchema.get(datasetName);
            if (userDSSchema == null) userDSSchema = FlexibleDatasetSchema.EMPTY;
            FlexibleDatasetSchema.TableField tbField = createTable(table, datasetName,
                    userDSSchema.getFieldByName(table.getName()),errors);
            //schemaConverter.convert(tbField,imp.asName.orElse(table.getName()))
            return new SourceTableImport(tableName, table, tbField);
        }
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
                                                         ProcessBundle<SchemaConversionError> errors) {
        SchemaGenerator generator = new SchemaGenerator();
        SourceTableStatistics stats = table.getStatistics();
        if (userSchema==null) {
            if (stats.getCount()==0) {
                errors.add(SchemaConversionError.fatal(NamePath.of(datasetname),"We cannot infer schema for table [%s] due to lack of data. Need to provide user schema.", table.getName()));
            }
            userSchema = FlexibleDatasetSchema.TableField.empty(table.getName());
        }
        FlexibleDatasetSchema.TableField result = generator.mergeSchema(stats, userSchema,
                NamePath.of(datasetname, table.getName()));
        errors.merge(generator.getErrors());
        return result;
    }

}
