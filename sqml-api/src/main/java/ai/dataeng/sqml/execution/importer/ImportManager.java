package ai.dataeng.sqml.execution.importer;

import ai.dataeng.sqml.ingest.DatasetLookup;
import ai.dataeng.sqml.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.ingest.schema.SchemaConversionError;
import ai.dataeng.sqml.ingest.schema.external.SchemaDefinition;
import ai.dataeng.sqml.ingest.schema.external.SchemaImport;
import ai.dataeng.sqml.ingest.source.SourceDataset;
import ai.dataeng.sqml.ingest.source.SourceTable;
import ai.dataeng.sqml.ingest.stats.SchemaGenerator;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.StandardField;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.schema2.constraint.Constraint;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.NameCanonicalizer;
import ai.dataeng.sqml.schema2.name.NamePath;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;


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

    private static void addMapping(Name mapTo, ImportSchema.Mapping original, Map<Name, ImportSchema.Mapping> mappings,
                                   ConversionError.Bundle<SchemaConversionError> errors) {
        if (mappings.containsKey(mapTo)) {
            errors.add(SchemaConversionError.fatal(NamePath.ROOT,"Duplicate IMPORT name: %s. Import names must be unique.",mapTo));
        } else {
            mappings.put(mapTo,original);
        }
    }

    public Pair<ImportSchema, ConversionError.Bundle<SchemaConversionError>> createImportSchema() {
        //What we need to create
        Map<Name, FlexibleDatasetSchema> sourceSchemas = new HashMap<>();
        RelationType.Builder<StandardField> schemaBuilder = new RelationType.Builder();
        Map<Name, ImportSchema.Mapping> nameMapping = new HashMap<>();

        SchemaConverter schemaConverter = new SchemaConverter();
        ConversionError.Bundle<SchemaConversionError> errors = new ConversionError.Bundle<>();

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
                                schemaBuilder.add(new StandardField(imp.asName.orElse(imp.datasetName),localSchemaBuilder.build(),Collections.EMPTY_LIST));
                            }
                            break;
                        case TABLE:
                            StandardField table = scriptSchema.getFieldByName(imp.tableName);
                            if (table == null) {
                                errors.add(SchemaConversionError.fatal(NamePath.of(datasetName), "Unknown table: %s", imp.tableName));
                            } else {
                                StandardField tableRename = new StandardField(imp.asName.orElse(table.getName()), table.getType(), table.getConstraints());
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
                                schemaBuilder.add(new StandardField(imp.asName.orElse(imp.datasetName),localSchemaBuilder.build(),Collections.EMPTY_LIST));
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
        return new ImmutablePair<>(
                new ImportSchema(datasetLookup,sourceSchemas,schemaBuilder.build(),nameMapping),
                errors);
    }

    public FlexibleDatasetSchema.TableField createTable(SourceTable table, Name datasetname,
                                                        FlexibleDatasetSchema.TableField userSchema,
                                                        ConversionError.Bundle<SchemaConversionError> errors) {
        SchemaGenerator generator = new SchemaGenerator();
        if (userSchema==null) {
            userSchema = FlexibleDatasetSchema.TableField.empty(table.getName());
        }
        FlexibleDatasetSchema.TableField result = generator.mergeSchema(datasetLookup.getTableStatistics(table), userSchema,
                NamePath.of(datasetname, table.getName()));
        errors.merge(generator.getErrors());
        return result;
    }

    public void registerScript(@NonNull String name, @NonNull RelationType<StandardField> datasetSchema) {
        registerScript(toName(name), datasetSchema);
    }

    public void registerScript(@NonNull Name name, @NonNull RelationType<StandardField> datasetSchema) {
        Preconditions.checkArgument(scriptSchemas.containsKey(name),"Duplicate name for script import: %s", name);
        scriptSchemas.put(name,datasetSchema);
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
        Preconditions.checkArgument(schema!=null && !schema.isEmpty());
        this.userSchema = schema;
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
