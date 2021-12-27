package ai.dataeng.sqml.planner.operator;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.importer.ImportManager;
import ai.dataeng.sqml.execution.flink.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.execution.flink.ingest.schema.FlexibleSchemaHelper;
import ai.dataeng.sqml.execution.flink.ingest.schema.SchemaConversionError;
import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Dataset;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.LogicalPlanImpl;
import ai.dataeng.sqml.planner.Relationship;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.type.RelationType;
import ai.dataeng.sqml.type.basic.BasicType;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import ai.dataeng.sqml.type.basic.ProcessMessage.ProcessBundle;
import ai.dataeng.sqml.type.constraint.Cardinality;
import ai.dataeng.sqml.type.constraint.Constraint;
import ai.dataeng.sqml.type.constraint.ConstraintHelper;
import ai.dataeng.sqml.type.constraint.NotNull;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import lombok.AllArgsConstructor;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Resolve imports in SQRL scripts and produces the {@link LogicalPlanImpl.Node}s as sources.
 */
public class ImportResolver {
    public static final Name PARENT_RELATIONSHIP = Name.system("parent");

    private final ImportManager importManager;
//    private final LogicalPlanImpl logicalPlan;
    private final ProcessBundle<ProcessMessage> errors;

    public ImportResolver(ImportManager importManager) {
        this(importManager, null, null);
    }
    public ImportResolver(ImportManager importManager,
        LogicalPlanImpl logicalPlan,
        ProcessBundle<ProcessMessage> errors) {
        this.importManager = importManager;
//        this.logicalPlan = logicalPlan;
        this.errors = errors;
    }

    public ImportManager getImportManager() {
        return importManager;
    }

    public void resolveImport(ImportMode importMode, Name datasetName,
        Optional<Name> tableName, Optional<Name> asName,
        Namespace namespace, ProcessBundle<ProcessMessage> errors) {

        ProcessBundle<SchemaConversionError> schemaErrors = new ProcessBundle<>();
        if (importMode==ImportMode.DATASET || importMode==ImportMode.ALLTABLE) {
            List<ImportManager.TableImport> tblimports = importManager.importAllTables(datasetName, schemaErrors);

            List<Table> tables = new ArrayList<>();
            for (ImportManager.TableImport tblimport : tblimports) {
                Table table = createTable(namespace, tblimport, Optional.empty());
                tables.add(table);
            }
            Dataset ds = new Dataset(asName.orElse(datasetName), tables);
            if (importMode == ImportMode.DATASET) {
                namespace.addDataset(ds);
            } else {
                namespace.scope(ds);
            }
        } else {
            assert importMode == ImportMode.TABLE;
            ImportManager.TableImport tblimport = importManager.importTable(datasetName, tableName.get(), schemaErrors);
            Table table = createTable(namespace, tblimport, asName);
            List<Table> tables = new ArrayList<>();
            tables.add(table);
            Dataset ds = new Dataset(asName.orElse(datasetName), tables);
            namespace.addDataset(ds);
        }
        errors.addAll(schemaErrors);
    }

    public enum ImportMode {
        DATASET, TABLE, ALLTABLE;
    }

    private Table createTable(Namespace namespace, ImportManager.TableImport tblImport, Optional<Name> asName) {
        if (tblImport instanceof ImportManager.SourceTableImport) {
            ImportManager.SourceTableImport sourceImport = (ImportManager.SourceTableImport) tblImport;

            Map<NamePath, Column[]> outputSchema = new HashMap<>();
            Table rootTable = tableConversion(namespace, sourceImport.getSourceSchema().getFields(),outputSchema,
                    asName.orElse(tblImport.getTableName()), NamePath.ROOT, null);
            DocumentSource source = new DocumentSource(sourceImport.getSourceSchema(), sourceImport.getTable(),outputSchema);
            namespace.addSourceNode(source);
            //Add shredder for each entry in outputSchema
            for (Map.Entry<NamePath, Column[]> entry : outputSchema.entrySet()) {
                ShreddingOperator.shredAtPath(source, entry.getKey(), rootTable);
            }
            return rootTable;
        } else {
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }

    private Table tableConversion(Namespace namespace, RelationType<FlexibleDatasetSchema.FlexibleField> relation,
                                              Map<NamePath, Column[]> outputSchema,
                                              Name name, NamePath path, Table parent) {
        if (parent!=null) name = Name.combine(parent.getName(),name);
        //Only the root table (i.e. without a parent) is visible in the schema
        Table table = namespace.createTable(name, parent!=null);
        List<Column> columns = new ArrayList<>();
        for (FlexibleDatasetSchema.FlexibleField field : relation) {
            for (Field f : fieldConversion(namespace, field, outputSchema, path, table)) {
                table.fields.add(f);
                if (f instanceof Column) columns.add((Column) f);
            }
        }
        outputSchema.put(path,columns.toArray(new Column[columns.size()]));
        return table;
    }

    private List<Field> fieldConversion(Namespace namespace, FlexibleDatasetSchema.FlexibleField field,
                                                    Map<NamePath, Column[]> outputSchema,
                                                    NamePath path, Table parent) {
        List<Field> result = new ArrayList<>(field.getTypes().size());
        for (FlexibleDatasetSchema.FieldType ft : field.getTypes()) {
            result.add(fieldTypeConversion(namespace, field,ft, field.getTypes().size()>1, outputSchema, path, parent));
        }
        return result;
    }

    private Field fieldTypeConversion(Namespace namespace, FlexibleDatasetSchema.FlexibleField field, FlexibleDatasetSchema.FieldType ftype,
                                                  final boolean isMixedType,
                                                  Map<NamePath, Column[]> outputSchema,
                                                  NamePath path, Table parent) {
        List<Constraint> constraints = ftype.getConstraints().stream()
                .filter(c -> {
                    //Since we map mixed types onto multiple fields, not-null no longer applies
                    if (c instanceof NotNull && isMixedType) return false;
                    return true;
                })
                .collect(Collectors.toList());
        Name name = FlexibleSchemaHelper.getCombinedName(field,ftype);

        if (ftype.getType() instanceof RelationType) {
            Table table = tableConversion(namespace, (RelationType<FlexibleDatasetSchema.FlexibleField>) ftype.getType(),
                    outputSchema, name, path.resolve(name), parent);
            //Add parent relationship
            table.fields.add(new Relationship(PARENT_RELATIONSHIP,table, parent,
                    Relationship.Type.PARENT, Relationship.Multiplicity.ONE));
            //Return child relationship
            Relationship.Multiplicity multiplicity = Relationship.Multiplicity.MANY;
            Cardinality cardinality = ConstraintHelper.getConstraint(constraints, Cardinality.class).orElse(Cardinality.UNCONSTRAINED);
            if (cardinality.isSingleton()) {
                multiplicity = Relationship.Multiplicity.ZERO_ONE;
                if (cardinality.isNonZero()) {
                    multiplicity = Relationship.Multiplicity.ONE;
                }
            }
            return new Relationship(name, parent, table,
                    Relationship.Type.CHILD, multiplicity);
        } else {
            assert ftype.getType() instanceof BasicType;
            return new Column(name, parent,0,(BasicType)ftype.getType(),ftype.getArrayDepth(), constraints, false, false);
        }
    }


}
