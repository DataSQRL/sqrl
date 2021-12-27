package ai.dataeng.sqml.planner.operator;

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
@AllArgsConstructor
public class ImportResolver {

    private static final Name PARENT_RELATIONSHIP = Name.system("parent");

    private final ImportManager importManager;
    private final LogicalPlanImpl logicalPlan;
    private final ProcessBundle<ProcessMessage> errors;

    public void resolveImport(ImportMode importMode, Name datasetName,
                                  Optional<Name> tableName, Optional<Name> asName) {
        ProcessBundle<SchemaConversionError> schemaErrors = new ProcessBundle<>();
        if (importMode==ImportMode.DATASET || importMode==ImportMode.ALLTABLE) {
            List<ImportManager.TableImport> tblimports = importManager.importAllTables(datasetName, schemaErrors);
            Dataset dataset = null;
            if (importMode == ImportMode.DATASET) {
                dataset = new Dataset(asName.orElse(datasetName));
                logicalPlan.schema.add(dataset);
            }
            for (ImportManager.TableImport tblimport : tblimports) {
                Table table = createTable(tblimport, Optional.empty());
                if (dataset!=null) dataset.tables.add(table);
                else logicalPlan.schema.add(table);
            }
        } else {
            assert importMode == ImportMode.TABLE;
            ImportManager.TableImport tblimport = importManager.importTable(datasetName, tableName.get(), schemaErrors);
            createTable(tblimport, asName);
        }
        errors.addAll(schemaErrors);
    }

    public enum ImportMode {

        DATASET, TABLE, ALLTABLE;

    }

    private Table createTable(ImportManager.TableImport tblImport, Optional<Name> asName) {
        if (tblImport instanceof ImportManager.SourceTableImport) {
            ImportManager.SourceTableImport sourceImport = (ImportManager.SourceTableImport) tblImport;

            Map<NamePath, Column[]> outputSchema = new HashMap<>();
            Table rootTable = tableConversion(sourceImport.getSourceSchema().getFields(),outputSchema,
                    asName.orElse(tblImport.getTableName()), NamePath.ROOT, null);
            DocumentSource source = new DocumentSource(sourceImport.getSourceSchema(), sourceImport.getTable(),outputSchema);
            logicalPlan.sourceNodes.add(source);
            //Add shredder for each entry in outputSchema
            for (Map.Entry<NamePath, Column[]> entry : outputSchema.entrySet()) {
                ShreddingOperator.shredAtPath(source, entry.getKey(), rootTable);
            }
            return rootTable;
        } else {
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }

    private Table tableConversion(RelationType<FlexibleDatasetSchema.FlexibleField> relation,
                                              Map<NamePath, Column[]> outputSchema,
                                              Name name, NamePath path, Table parent) {
        if (parent!=null) name = Name.combine(parent.getName(),name);
        //Only the root table (i.e. without a parent) is visible in the schema
        Table table = logicalPlan.createTable(name, parent!=null);
        List<Column> columns = new ArrayList<>();
        for (FlexibleDatasetSchema.FlexibleField field : relation) {
            for (Field f : fieldConversion(field, outputSchema, path, table)) {
                table.fields.add(f);
                if (f instanceof Column) columns.add((Column) f);
            }
        }
        outputSchema.put(path,columns.toArray(new Column[columns.size()]));
        return table;
    }

    private List<Field> fieldConversion(FlexibleDatasetSchema.FlexibleField field,
                                                    Map<NamePath, Column[]> outputSchema,
                                                    NamePath path, Table parent) {
        List<Field> result = new ArrayList<>(field.getTypes().size());
        for (FlexibleDatasetSchema.FieldType ft : field.getTypes()) {
            result.add(fieldTypeConversion(field,ft, field.getTypes().size()>1, outputSchema, path, parent));
        }
        return result;
    }

    private Field fieldTypeConversion(FlexibleDatasetSchema.FlexibleField field, FlexibleDatasetSchema.FieldType ftype,
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
            Table table = tableConversion((RelationType<FlexibleDatasetSchema.FlexibleField>) ftype.getType(),
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
