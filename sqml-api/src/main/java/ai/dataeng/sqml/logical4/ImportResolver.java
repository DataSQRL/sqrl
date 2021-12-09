package ai.dataeng.sqml.logical4;

import ai.dataeng.sqml.execution.importer.ImportManager;
import ai.dataeng.sqml.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.ingest.schema.FlexibleSchemaHelper;
import ai.dataeng.sqml.ingest.schema.SchemaConversionError;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.basic.BasicType;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.schema2.constraint.Cardinality;
import ai.dataeng.sqml.schema2.constraint.Constraint;
import ai.dataeng.sqml.schema2.constraint.ConstraintHelper;
import ai.dataeng.sqml.schema2.constraint.NotNull;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import lombok.AllArgsConstructor;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Resolve imports in SQRL scripts and produces the {@link LogicalPlan.Node}s as sources.
 */
@AllArgsConstructor
public class ImportResolver {

    private static final Name PARENT_RELATIONSHIP = Name.system("parent");

    private final ImportManager importManager;
    private final LogicalPlan logicalPlan;
    private final ConversionError.Bundle<ConversionError> errors;

    public void resolveImport(ImportMode importMode, Name datasetName,
                                  Optional<Name> tableName, Optional<Name> asName) {
        ConversionError.Bundle<SchemaConversionError> schemaErrors = new ConversionError.Bundle<>();
        if (importMode==ImportMode.DATASET || importMode==ImportMode.ALLTABLE) {
            List<ImportManager.TableImport> tblimports = importManager.importAllTables(datasetName, schemaErrors);
            LogicalPlan.Dataset dataset = null;
            if (importMode == ImportMode.DATASET) {
                dataset = new LogicalPlan.Dataset(asName.orElse(datasetName));
                logicalPlan.schema.add(dataset);
            }
            for (ImportManager.TableImport tblimport : tblimports) {
                LogicalPlan.Table table = createTable(tblimport, Optional.empty());
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

    private LogicalPlan.Table createTable(ImportManager.TableImport tblImport, Optional<Name> asName) {
        if (tblImport instanceof ImportManager.SourceTableImport) {
            ImportManager.SourceTableImport sourceImport = (ImportManager.SourceTableImport) tblImport;

            Map<NamePath, LogicalPlan.Column[]> outputSchema = new HashMap<>();
            LogicalPlan.Table rootTable = tableConversion(sourceImport.getSourceSchema().getFields(),outputSchema,
                    asName.orElse(tblImport.getTableName()), NamePath.ROOT, null);
            DocumentSource source = new DocumentSource(sourceImport.getSourceSchema(), sourceImport.getTable(),outputSchema);
            logicalPlan.sourceNodes.add(source);
            //Add shredder for each entry in outputSchema
            for (Map.Entry<NamePath, LogicalPlan.Column[]> entry : outputSchema.entrySet()) {
                ShreddingOperator.shredAtPath(source, entry.getKey(), rootTable);
            }
            return rootTable;
        } else {
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }

    private LogicalPlan.Table tableConversion(RelationType<FlexibleDatasetSchema.FlexibleField> relation,
                                              Map<NamePath, LogicalPlan.Column[]> outputSchema,
                                              Name name, NamePath path, LogicalPlan.Table parent) {
        if (parent!=null) name = Name.combine(parent.getName(),name);
        //Only the root table (i.e. without a parent) is visible in the schema
        LogicalPlan.Table table = logicalPlan.createTable(name, parent!=null);
        List<LogicalPlan.Column> columns = new ArrayList<>();
        for (FlexibleDatasetSchema.FlexibleField field : relation) {
            for (LogicalPlan.Field f : fieldConversion(field, outputSchema, path, table)) {
                table.fields.add(f);
                if (f instanceof LogicalPlan.Column) columns.add((LogicalPlan.Column) f);
            }
        }
        outputSchema.put(path,columns.toArray(new LogicalPlan.Column[columns.size()]));
        return table;
    }

    private List<LogicalPlan.Field> fieldConversion(FlexibleDatasetSchema.FlexibleField field,
                                                    Map<NamePath, LogicalPlan.Column[]> outputSchema,
                                                    NamePath path, LogicalPlan.Table parent) {
        List<LogicalPlan.Field> result = new ArrayList<>(field.getTypes().size());
        for (FlexibleDatasetSchema.FieldType ft : field.getTypes()) {
            result.add(fieldTypeConversion(field,ft, field.getTypes().size()>1, outputSchema, path, parent));
        }
        return result;
    }

    private LogicalPlan.Field fieldTypeConversion(FlexibleDatasetSchema.FlexibleField field, FlexibleDatasetSchema.FieldType ftype,
                                                  final boolean isMixedType,
                                                  Map<NamePath, LogicalPlan.Column[]> outputSchema,
                                                  NamePath path, LogicalPlan.Table parent) {
        List<Constraint> constraints = ftype.getConstraints().stream()
                .filter(c -> {
                    //Since we map mixed types onto multiple fields, not-null no longer applies
                    if (c instanceof NotNull && isMixedType) return false;
                    return true;
                })
                .collect(Collectors.toList());
        Name name = FlexibleSchemaHelper.getCombinedName(field,ftype);

        if (ftype.getType() instanceof RelationType) {
            LogicalPlan.Table table = tableConversion((RelationType<FlexibleDatasetSchema.FlexibleField>) ftype.getType(),
                    outputSchema, name, path.resolve(name), parent);
            //Add parent relationship
            table.fields.add(new LogicalPlan.Relationship(PARENT_RELATIONSHIP,parent,
                    LogicalPlan.Relationship.Type.PARENT, LogicalPlan.Relationship.Multiplicity.ONE));
            //Return child relationship
            LogicalPlan.Relationship.Multiplicity multiplicity = LogicalPlan.Relationship.Multiplicity.MANY;
            Cardinality cardinality = ConstraintHelper.getConstraint(constraints, Cardinality.class).orElse(Cardinality.UNCONSTRAINED);
            if (cardinality.isSingleton()) {
                multiplicity = LogicalPlan.Relationship.Multiplicity.ZERO_ONE;
                if (cardinality.isNonZero()) {
                    multiplicity = LogicalPlan.Relationship.Multiplicity.ONE;
                }
            }
            return new LogicalPlan.Relationship(name, table,
                    LogicalPlan.Relationship.Type.CHILD, multiplicity);
        } else {
            assert ftype.getType() instanceof BasicType;
            return new LogicalPlan.Column(name, parent,0,(BasicType)ftype.getType(),ftype.getArrayDepth(), constraints, false, false);
        }
    }


}
