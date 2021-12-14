package ai.dataeng.sqml.execution.importer;

import ai.dataeng.sqml.ingest.DatasetLookup;
import ai.dataeng.sqml.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.ingest.schema.SchemaConversionError;
import ai.dataeng.sqml.ingest.schema.external.SchemaDefinition;
import ai.dataeng.sqml.ingest.schema.external.SchemaImport;
import ai.dataeng.sqml.ingest.source.SourceDataset;
import ai.dataeng.sqml.ingest.source.SourceTable;
import ai.dataeng.sqml.ingest.stats.SchemaGenerator;
import ai.dataeng.sqml.ingest.stats.SourceTableStatistics;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.StandardField;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.schema2.constraint.Constraint;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import org.jooq.Table;


import java.util.*;
import java.util.stream.Collectors;

public class ImportManager {

    public static final NameCanonicalizer IMPORT_CANONICALIZER = NameCanonicalizer.SYSTEM;

    private final DatasetLookup datasetLookup;
    private Map<Name, FlexibleDatasetSchema> userSchema = Collections.EMPTY_MAP;
    private Map<Name, RelationType<StandardField>> scriptSchemas = new HashMap<>();

    private final List<ImportDirective> imports = new ArrayList<>();

    public ImportManager(DatasetLookup datasetLookup) {
        this.datasetLookup = datasetLookup;
    }

    public ConversionError.Bundle<SchemaConversionError> registerUserSchema(SchemaDefinition yamlSchema) {
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
                                             ConversionError.Bundle<SchemaConversionError> errors) {
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

    public TableImport importTable(@NonNull Name datasetName,  @NonNull Name tableName,
                                   ConversionError.Bundle<SchemaConversionError> errors) {
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
                                                         ConversionError.Bundle<SchemaConversionError> errors) {
        SchemaGenerator generator = new SchemaGenerator();
        SourceTableStatistics stats = datasetLookup.getTableStatistics(table);
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

    //================= OLD (can be deleted once refactoring is complete) ========================

    public ImportSchema createImportSchema(ConversionError.Bundle<SchemaConversionError> errors) {
        //What we need to create
        Map<Name, FlexibleDatasetSchema> sourceSchemas = new HashMap<>();
        RelationType.Builder<StandardField> schemaBuilder = new RelationType.Builder();
        Map<Name, ImportSchema.Mapping> nameMapping = new HashMap<>();

        SchemaConverter schemaConverter = new SchemaConverter();

        //First, lets group imports by dataset
        Map<Name,List<ImportDirective>> importsByDataset = imports.stream().collect(Collectors.groupingBy(ImportDirective::getDatasetName));
        //Now, process one dataset at a time
        for (Map.Entry<Name,List<ImportDirective>> group : importsByDataset.entrySet()) {
            Name datasetName = group.getKey();
            //Determine if we are importing a script or SourceDataset - check scripts first
            if (scriptSchemas.containsKey(datasetName)) {
                RelationType<StandardField> scriptSchema = scriptSchemas.get(datasetName);

                //TODO: Should we also validate the inferred script schema against user provided schema?
                RelationType.Builder<StandardField> localSchemaBuilder = schemaBuilder;
                for (ImportDirective imp : group.getValue()) {
                    boolean importLocal = true;
                    switch (imp.mode) {
                        case DATASET:
                            importLocal = false;
                            localSchemaBuilder = new RelationType.Builder<>();
                            addMapping(imp.asName.orElse(datasetName),
                                    new ImportSchema.Mapping(ImportSchema.ImportType.SCRIPT, datasetName,null),
                                    nameMapping, errors);
                        case ALLTABLE:
                            for (StandardField table : scriptSchema) {
                                localSchemaBuilder.add(table);
                                if (importLocal) {
                                    addMapping(table.getName(),
                                            new ImportSchema.Mapping(ImportSchema.ImportType.SCRIPT, datasetName, table.getName()),
                                            nameMapping, errors);
                                }
                            }
                            if (!importLocal) {
                                schemaBuilder.add(new StandardField(imp.asName.orElse(imp.datasetName),localSchemaBuilder.build(),Collections.EMPTY_LIST, Optional.empty()));
                            }
                            break;
                        case TABLE:
                            StandardField table = scriptSchema.getFieldByName(imp.tableName);
                            if (table == null) {
                                errors.add(SchemaConversionError.fatal(NamePath.of(datasetName), "Unknown table: %s", imp.tableName));
                            } else {
                                StandardField tableRename = new StandardField(imp.asName.orElse(table.getName()), table.getType(), table.getConstraints(), Optional.empty());
                                localSchemaBuilder.add(tableRename);
                                addMapping(imp.asName.orElse(table.getName()),
                                        new ImportSchema.Mapping(ImportSchema.ImportType.SCRIPT, datasetName, table.getName()),
                                        nameMapping, errors);
                            }
                            break;
                        default:
                            throw new RuntimeException("Encountered unexpected mode: " + imp.mode);
                    }
                }
            } else { //Assume it's a SourceDataset
                SourceDataset dsDef = datasetLookup.getDataset(datasetName);
                if (dsDef == null) {
                    errors.add(SchemaConversionError.fatal(NamePath.ROOT, "Unknown dataset: %s", datasetName));
                    continue;
                }

                FlexibleDatasetSchema userDSSchema = userSchema.get(datasetName);
                if (userDSSchema == null) userDSSchema = FlexibleDatasetSchema.EMPTY;
                FlexibleDatasetSchema.Builder sourceSchema = new FlexibleDatasetSchema.Builder();
                sourceSchema.setDescription(userDSSchema.getDescription());

                RelationType.Builder<StandardField> localSchemaBuilder = schemaBuilder;

                for (ImportDirective imp : group.getValue()) {
                    boolean importLocal = true;
                    switch (imp.mode) {
                        case DATASET:
                            importLocal = false;
                            localSchemaBuilder = new RelationType.Builder<>();
                            addMapping(imp.asName.orElse(datasetName),
                                    new ImportSchema.Mapping(ImportSchema.ImportType.SOURCE, datasetName,null),
                                    nameMapping, errors);
                        case ALLTABLE:
                            for (SourceTable table : dsDef.getTables()) {
                                FlexibleDatasetSchema.TableField tbField = createTable(table, datasetName,
                                        userDSSchema.getFieldByName(table.getName()),errors);
                                sourceSchema.add(tbField);
                                localSchemaBuilder.add(schemaConverter.convert(tbField));
                                if (importLocal) {
                                    addMapping(table.getName(),
                                            new ImportSchema.Mapping(ImportSchema.ImportType.SOURCE, datasetName,table.getName()),
                                            nameMapping, errors);
                                }
                            }
                            if (!importLocal) {
                                schemaBuilder.add(new StandardField(imp.asName.orElse(imp.datasetName),localSchemaBuilder.build(),Collections.EMPTY_LIST, Optional.empty()));
                            }
                            break;
                        case TABLE:
                            SourceTable table = dsDef.getTable(imp.tableName);
                            if (table == null) {
                                errors.add(SchemaConversionError.fatal(NamePath.of(datasetName), "Unknown table: %s", imp.tableName));
                            } else {
                                FlexibleDatasetSchema.TableField tbField = createTable(table, datasetName,
                                        userDSSchema.getFieldByName(table.getName()),errors);
                                sourceSchema.add(tbField);
                                localSchemaBuilder.add(schemaConverter.convert(tbField,imp.asName.orElse(table.getName())));
                                addMapping(imp.asName.orElse(table.getName()),
                                        new ImportSchema.Mapping(ImportSchema.ImportType.SOURCE, datasetName, table.getName()),
                                        nameMapping, errors);
                            }
                            break;
                        default:
                            throw new RuntimeException("Encountered unexpected mode: " + imp.mode);
                    }
                }
                sourceSchemas.put(datasetName,sourceSchema.build());
            }
        }
        return new ImportSchema(datasetLookup,sourceSchemas,schemaBuilder.build(),nameMapping);
    }

    private static void addMapping(Name mapTo, ImportSchema.Mapping original, Map<Name, ImportSchema.Mapping> mappings,
                                   ConversionError.Bundle<SchemaConversionError> errors) {
        if (mappings.containsKey(mapTo)) {
            errors.add(SchemaConversionError.fatal(NamePath.ROOT,"Duplicate IMPORT name: %s. Import names must be unique.",mapTo));
        } else {
            mappings.put(mapTo,original);
        }
    }




    /**
     * For import statements that have the structure
     * IMPORT datasetName [AS asName];
     * @param datasetName
     * @param asName
     */
    public void importDataset(@NonNull String datasetName, Optional<String> asName) {
        importDataset(toName(datasetName),asName.map(ImportManager::toName));
    }

    public void importDataset(@NonNull Name datasetName, Optional<Name> asName) {
        addImport(new ImportDirective(ImportMode.DATASET, datasetName, null, asName));
    }

    /**
     * For import statements that have the structure
     * IMPORT datasetName.tableName [AS asTableName];
     * @param datasetName
     * @param tableName
     * @param asTableName
     */
    public void importTable(@NonNull String datasetName,  @NonNull String tableName, Optional<String> asTableName) {
        importTable(toName(datasetName),toName(tableName),asTableName.map(ImportManager::toName));
    }

    public void importTable(@NonNull Name datasetName,  @NonNull Name tableName, Optional<Name> asTableName) {
        addImport(new ImportDirective(ImportMode.TABLE, datasetName, tableName, asTableName));
    }

    /**
     * For import statements that have the structure
     * IMPORT datasetName.*;
     * @param datasetName
     */
    public void importAllTable(@NonNull String datasetName) {
        importAllTable(toName(datasetName));
    }

    public void importAllTable(@NonNull Name datasetName) {
        addImport(new ImportDirective(ImportMode.ALLTABLE, datasetName, null, Optional.empty()));
    }

    private void addImport(@NonNull ImportDirective importDirective) {
        imports.add(importDirective);
    }

    @Value
    private static class ImportDirective {

        @NonNull
        final ImportMode mode;
        @NonNull
        final Name datasetName;
        final Name tableName;
        @NonNull
        final Optional<Name> asName;

    }

    public static enum ImportMode {

        DATASET, TABLE, ALLTABLE;

    }

    private static Name toName(@NonNull String name) {
        return Name.of(name,IMPORT_CANONICALIZER);
    }


}
