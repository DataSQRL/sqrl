package ai.dataeng.sqml.planner.operator;

import static ai.dataeng.sqml.tree.name.Name.PARENT_RELATIONSHIP;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.planner.AliasGenerator;
import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Dataset;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.LogicalPlanImpl;
import ai.dataeng.sqml.planner.LogicalPlanUtil;
import ai.dataeng.sqml.planner.Planner;
import ai.dataeng.sqml.planner.RelToSql;
import ai.dataeng.sqml.planner.Relationship;
import ai.dataeng.sqml.planner.Relationship.Type;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.planner.operator2.SqrlRelNode;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import ai.dataeng.sqml.type.RelationType;
import ai.dataeng.sqml.type.basic.BasicType;
import ai.dataeng.sqml.type.basic.DateTimeType;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import ai.dataeng.sqml.type.basic.ProcessMessage.ProcessBundle;
import ai.dataeng.sqml.type.constraint.Cardinality;
import ai.dataeng.sqml.type.constraint.Constraint;
import ai.dataeng.sqml.type.constraint.ConstraintHelper;
import ai.dataeng.sqml.type.constraint.NotNull;
import ai.dataeng.sqml.type.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.type.schema.FlexibleSchemaHelper;
import ai.dataeng.sqml.type.schema.SchemaConversionError;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.OperatorTable;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.SqrlRelBuilder;
import org.apache.commons.math3.util.Pair;

/**
 * Resolve imports in SQRL scripts and produces the {@link LogicalPlanImpl.Node}s as sources.
 */
public class ImportResolver {
    private final ImportManager importManager;
    private final ProcessBundle<ProcessMessage> errors;
    private final Planner planner;

    public ImportResolver(ImportManager importManager, Planner planner) {
        this(importManager, null, planner);
    }
    public ImportResolver(ImportManager importManager,
        ProcessBundle<ProcessMessage> errors, Planner planner) {
        this.importManager = importManager;
        this.errors = errors;
        this.planner = planner;
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
            if (importMode == ImportMode.ALLTABLE) {
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
        DATASET, TABLE, ALLTABLE
    }

    private Table createTable(Namespace namespace, ImportManager.TableImport tblImport, Optional<Name> asName) {
        if (tblImport instanceof ImportManager.SourceTableImport) {
            ImportManager.SourceTableImport sourceImport = (ImportManager.SourceTableImport) tblImport;

            Map<NamePath, Column[]> outputSchema = new HashMap<>();
            Table rootTable = tableConversion(namespace, sourceImport.getSourceSchema().getFields(),outputSchema,
                    asName.orElse(tblImport.getTableName()), NamePath.ROOT, null);
            DocumentSource source = new DocumentSource(sourceImport.getSourceSchema(), sourceImport.getTable(), outputSchema);
            //Add shredder for each entry in outputSchema
            for (Map.Entry<NamePath, Column[]> entry : outputSchema.entrySet()) {
                ShreddingOperator shred = ShreddingOperator.shredAtPath(source, entry.getKey(), rootTable);
            }


            namespace.addSourceNode(source);

            //Set relational nodes
            for (Map.Entry<NamePath, Column[]> entry : outputSchema.entrySet()) {
                Column[] inputSchema = source.getOutputSchema().get(entry.getKey());
                assert inputSchema!=null && inputSchema.length>0;

                Table targetTable = LogicalPlanUtil.getTable(inputSchema);

                SqrlRelNode node = (SqrlRelNode) planner.getRelBuilder(Optional.empty(), namespace)
                    .scan_base(targetTable.getPath().toString())
                    .build();
                targetTable.setRelNode(node);
                ShadowingContainer<Field> fields = new ShadowingContainer<>();
                fields.addAll(node.getFields());
                targetTable.fields = fields;
            }
            setParentChildRelation(rootTable, namespace);

            return rootTable;
        } else {
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }

    private Table tableConversion(Namespace namespace, RelationType<FlexibleDatasetSchema.FlexibleField> relation,
                                              Map<NamePath, Column[]> outputSchema,
                                              Name name, NamePath path, Table parent) {
        NamePath namePath = getNamePath(name, Optional.ofNullable(parent));

        //Only the root table (i.e. without a parent) is visible in the schema
        Table table = namespace.createTable(name, namePath, parent!=null);
        List<Column> columns = new ArrayList<>();
        for (FlexibleDatasetSchema.FlexibleField field : relation) {
            for (Field f : fieldConversion(namespace, field, outputSchema, path, table)) {
                table.fields.add(f);
                if (f instanceof Column) columns.add((Column) f);
            }
        }
        Column ingestTime = Column.createTemp("_ingest_time", DateTimeType.INSTANCE, table);
        columns.add(ingestTime);
        table.fields.add(ingestTime);
        outputSchema.put(path,columns.toArray(new Column[columns.size()]));
        return table;
    }

    private void setParentChildRelation(Table table, Namespace namespace) {
        for (Field field : table.getFields()) {
            //RelNode added after added to table
            if (field instanceof Relationship) {
                Relationship col = (Relationship) field;
                //When adding a direct child reference, create parent reference as if it
                //was it's inverse.

                AliasGenerator gen = new AliasGenerator();
                if (col.getType() == Type.CHILD) {
                    SqlIdentifier left = new SqlIdentifier(col.getTable().getPath().toString(), SqlParserPos.ZERO);
                    String la = gen.nextAlias();
                    SqlIdentifier left_alias = new SqlIdentifier(la, SqlParserPos.ZERO);
                    SqlIdentifier right = new SqlIdentifier(col.getToTable().getPath().toString(), SqlParserPos.ZERO);
                    String ra = gen.nextAlias();
                    SqlIdentifier right_alias = new SqlIdentifier(ra, SqlParserPos.ZERO);

                    SqlIdentifier[] l = new SqlIdentifier[]{left, left_alias};
                    SqlIdentifier[] r = new SqlIdentifier[]{right, right_alias};
//                    SqlNode[] condition = new SqlNode[col.getToTable().getForeignKeys().size()];
                    List<Column> foreignKeys = col.getToTable().getForeignKeys();
                    SqlBasicCall[] conditions = new SqlBasicCall[foreignKeys.size()];
                    for (int i = 0; i < foreignKeys.size(); i++) {
                        Column column = foreignKeys.get(i);
                        SqlNode[] condition = {
                            new SqlIdentifier(List.of(la, column.getFkReferences().get().getId()), SqlParserPos.ZERO),
                            new SqlIdentifier(List.of(ra, column.getId()), SqlParserPos.ZERO),
                        };
                        SqlBasicCall call = new SqlBasicCall(OperatorTable.EQUALS, condition, SqlParserPos.ZERO);
                        conditions[i] = call;
                    }
                    SqlNode condition;
                    if (foreignKeys.size() == 1) {
                        condition = conditions[0];
                    } else {
                        condition = new SqlBasicCall(OperatorTable.AND, conditions, SqlParserPos.ZERO);
                    }

                    SqlJoin join = new SqlJoin(SqlParserPos.ZERO,
                        new SqlBasicCall(OperatorTable.AS, l, SqlParserPos.ZERO),
                        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
                        SqlLiteral.createSymbol(JoinType.INNER, SqlParserPos.ZERO),
                        new SqlBasicCall(OperatorTable.AS, r, SqlParserPos.ZERO),
                        SqlLiteral.createSymbol(JoinConditionType.ON, SqlParserPos.ZERO),
                        condition
                    );
                    SqlValidator validator = planner.getValidator(namespace);
                    SqlSelect select = new SqlSelect(SqlParserPos.ZERO,
                        new SqlNodeList(SqlParserPos.ZERO),
                        new SqlNodeList(List.of(new SqlIdentifier(List.of(la), SqlParserPos.ZERO).plusStar()), SqlParserPos.ZERO),
                        join, null, null, null, null, null, null, null, null
                        );

                    validator.validate(select);

                    List<Pair<String, String>> keys = new ArrayList();
                    for (Column column : foreignKeys) {
                        keys.add(Pair.create(column.getId(), column.getFkReferences().get().getId()));
                    }

                    col.setSqlNode(join);

                    setParentChildRelation(col.toTable, namespace);
                } else if (col.getType() == Type.PARENT) {
                    SqrlRelBuilder relBuilder = planner.getRelBuilder(Optional.empty(), namespace);
                    relBuilder.scan_base(col.table.getPath().toString());
                    relBuilder.scan_base(col.toTable.getPath().toString());
                    List<RexNode> conditions = buildParentChildConditions(col.table, relBuilder);
                    relBuilder.join(JoinRelType.INNER, conditions);
                    RelNode node = relBuilder.build();
                    System.out.println(RelToSql.convertToSql(node));
                    System.out.println(node.explain());
                    col.setNode(node);
                }

                //TODO: Parent relations
//                else if (col.getType() == Type.PARENT) {
//                    RelNode node = buildParentChildRel(col.getToTable(), col.getTable(), namespace);
//                    col.setNode(node);
//                }
            }
        }
    }

    private List<RexNode> buildParentChildConditions(Table toTable, RelBuilder relBuilder) {
        List<RexNode> conditions = new ArrayList<>();
        for (Column column : toTable.getForeignKeys()) {
            if (column.isForeignKey) {
                RexInputRef leftRef = relBuilder.field(2, 0,
                    column.getFkReferences().get().getId());
                RexInputRef rightRef = relBuilder.field(2, 1,
                    column.getId());
                conditions.add(relBuilder.equals(leftRef, rightRef));
            }
        }
        if (conditions.isEmpty()) {
            throw new RuntimeException("Could not find primary keys when joining parent/child");
        }
        return conditions;

    }

    private NamePath getNamePath(Name name, Optional<Table> parent) {
        NamePath namePath;
        if (parent.isPresent()) {
            namePath = parent.get().getPath().resolve(name);
        } else {
            namePath = NamePath.of(name);
        }
        return namePath;
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
            Relationship parentField = new Relationship(PARENT_RELATIONSHIP, table, parent,
                Relationship.Type.PARENT, Relationship.Multiplicity.ONE, null, null);
            table.fields.add(parentField);
            //Return child relationship
            Relationship.Multiplicity multiplicity = Relationship.Multiplicity.MANY;
            Cardinality cardinality = ConstraintHelper.getConstraint(constraints, Cardinality.class).orElse(Cardinality.UNCONSTRAINED);
            if (cardinality.isSingleton()) {
                multiplicity = Relationship.Multiplicity.ZERO_ONE;
                if (cardinality.isNonZero()) {
                    multiplicity = Relationship.Multiplicity.ONE;
                }
            }
            Relationship child = new Relationship(name, parent, table,
                Relationship.Type.CHILD, multiplicity, parentField, null);
            parentField.setInverse(child);
            return child;
        } else {
            assert ftype.getType() instanceof BasicType;
            return new Column(name, parent,0,(BasicType)ftype.getType(),ftype.getArrayDepth(), constraints, false,
                false, null, false);
        }
    }
}
