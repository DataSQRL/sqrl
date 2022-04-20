package ai.datasqrl.plan;

import static ai.datasqrl.parse.tree.name.Name.INGEST_TIME;

import ai.datasqrl.execute.flink.ingest.schema.FlinkTableConverter;
import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.DistinctAssignment;
import ai.datasqrl.parse.tree.ExpressionAssignment;
import ai.datasqrl.parse.tree.ImportDefinition;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.VersionedName;
import ai.datasqrl.plan.LocalPlanner.PlannerScope;
import ai.datasqrl.plan.calcite.CalcitePlanner;
import ai.datasqrl.plan.util.FlinkRelDataTypeConverter;
import ai.datasqrl.plan.util.FlinkSchemaUtil;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.operations.AddColumnOp;
import ai.datasqrl.schema.operations.AddDatasetOp;
import ai.datasqrl.schema.operations.AddQueryOp;
import ai.datasqrl.schema.operations.SchemaOperation;
import ai.datasqrl.validate.scopes.DistinctScope;
import ai.datasqrl.validate.scopes.ImportScope;
import ai.datasqrl.validate.scopes.StatementScope;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;

public class LocalPlanner extends AstVisitor<SchemaOperation, PlannerScope> {
  private final FlinkTableConverter tbConverter = new FlinkTableConverter();
  private final CalcitePlanner calcitePlanner;

  public LocalPlanner(ai.datasqrl.schema.Schema schema) {
    calcitePlanner = new CalcitePlanner(schema);
  }

  public SchemaOperation plan(Node node, SqlNode sqlNode,
      StatementScope scope) {
    return node.accept(this, createScope(scope, sqlNode));
  }

  private PlannerScope createScope(StatementScope scope, SqlNode sqlNode) {
    return new PlannerScope(scope, sqlNode);
  }

  @Value
  public class PlannerScope {
    StatementScope scope;
    SqlNode sqlNode;
  }
  /**
   * Import
   */
  @Override
  public SchemaOperation visitImportDefinition(ImportDefinition node, PlannerScope scope) {
    ImportScope importScope = (ImportScope) scope.getScope().getScopes().get(node);

    //Walk each discovered table, add NamePath Map
    Pair<Schema, TypeInformation> tbl = tbConverter
        .tableSchemaConversion(importScope.getSourceTableImport().getSourceSchema());
    Schema schema = tbl.getKey();

    //Import statements can be aliased
    Name tableName = node.getAliasName().orElse(importScope.getSourceTableImport().getTableName());

    List<ImportTable> importedPaths = new ArrayList<>();

    RelDataType streamRelType = FlinkRelDataTypeConverter.toRelDataType(schema.getColumns());
    RelNode relNode = calcitePlanner.createRelBuilder()
        .scanStream(tableName, streamRelType)
        .watermark(getIndex(tbl.getLeft(), INGEST_TIME.getCanonical()))
        .project(FlinkRelDataTypeConverter.getScalarIndexes(schema))
        .build();

    ImportTable table = new ImportTable(tableName.toNamePath(),
        relNode,
        FlinkSchemaUtil.getFieldNames(relNode),
        Set.of(getIndex(relNode.getRowType(), "_uuid")),
        Set.of());

    importedPaths.add(table);

    if (requiresShredding(schema)) {
      importedPaths.addAll(shred(tableName, streamRelType, schema));
    }

    return new AddDatasetOp(importedPaths);
  }

  private int getIndex(Schema schema, String name) {
    for (int i = 0; i < schema.getColumns().size(); i++) {
      if (schema.getColumns().get(i).getName().equalsIgnoreCase(name)) {
        return i;
      }
    }

    return -1;
  }
  private int getIndex(RelDataType type, String name) {
    for (int i = 0; i < type.getFieldList().size(); i++) {
      if (type.getFieldList().get(i).getName().equalsIgnoreCase(name)) {
        return i;
      }
    }

    return -1;
  }

  private List<ImportTable> shred(Name baseStream, RelDataType streamRelType, Schema schema) {
    List<UnresolvedColumn> columns = schema.getColumns();
    List<ImportTable> tables = new ArrayList<>();

    for (int i = 0; i < columns.size(); i++) {
      UnresolvedColumn col = columns.get(i);
      UnresolvedPhysicalColumn unresolvedPhysicalColumn = (UnresolvedPhysicalColumn) col;
      if (unresolvedPhysicalColumn.getDataType() instanceof FieldsDataType
          || unresolvedPhysicalColumn.getDataType() instanceof CollectionDataType) {
        FieldsDataType fieldsDataType =
            unresolvedPhysicalColumn.getDataType() instanceof CollectionDataType
                //unbox collection types
                ? (FieldsDataType) ((CollectionDataType) unresolvedPhysicalColumn.getDataType()).getElementDataType()
                : (FieldsDataType) unresolvedPhysicalColumn.getDataType();
        tables.add(shred(baseStream, unresolvedPhysicalColumn.getName(), i, fieldsDataType, streamRelType, schema));
      }
    }
    return tables;
  }

  private ImportTable shred(Name baseStream, String fieldName, int index, FieldsDataType type,
    RelDataType streamRelType, Schema schema) {
    List<RowField> fields = ((RowType)type.getLogicalType()).getFields();

    SqrlRelBuilder builder = calcitePlanner.createRelBuilder();
    RexBuilder rexBuilder = builder.getRexBuilder();
    CorrelationId id = new CorrelationId(0);
    int indexOfField = getIndex(streamRelType, fieldName);
    RelDataType t = FlinkTypeFactory.INSTANCE().createSqlType(SqlTypeName.INTEGER);

    RelBuilder b = builder
        .scanStream(baseStream, streamRelType)
        .watermark(getIndex(streamRelType, INGEST_TIME.getCanonical()))
        .values(List.of(List.of(rexBuilder.makeExactLiteral(BigDecimal.ZERO))),
            new RelRecordType(List.of(new RelDataTypeFieldImpl("ZERO", 0, t))))
        .project(List.of(builder.getRexBuilder().makeFieldAccess(
          rexBuilder.makeCorrel(streamRelType, id), fieldName, false
        )), List.of(fieldName))
        .uncollect(List.of(), false)
        .correlate(JoinRelType.INNER, id, RexInputRef.of(indexOfField, builder.peek().getRowType()))
        .project(projectShreddedColumns(rexBuilder, builder.peek()))
        ;

    RelNode node = b.build();
    System.out.println(node.explain());

    String columnName = fieldName;
    NamePath namePath = baseStream.toNamePath().concat(Name.system(columnName));
    ImportTable importTable = new ImportTable(namePath,
        node, FlinkSchemaUtil.getFieldNames(node),
        Set.of(getIndex(node.getRowType(), "_idx")),
        Set.of(0) //first index for uuid
    );

    return importTable;
  }

  private List<RexNode> projectShreddedColumns(RexBuilder rexBuilder,
      RelNode node) {
    List<RexNode> projects = new ArrayList<>();
    LogicalCorrelate correlate = (LogicalCorrelate) node;
    for (int i = 0; i < correlate.getLeft().getRowType().getFieldCount(); i++) {
      String name = correlate.getLeft().getRowType().getFieldNames().get(i);
      if (name.equalsIgnoreCase("_uuid")){//|| name.equalsIgnoreCase("_ingest_time")) {
        projects.add(rexBuilder.makeInputRef(node, i));
      }
    }

    //All columns on rhs
    for (int i = correlate.getLeft().getRowType().getFieldCount(); i < correlate.getRowType().getFieldCount(); i++) {
      projects.add(rexBuilder.makeInputRef(node, i));
    }

    return projects;
  }

  private boolean requiresShredding(Schema schema) {
    for (UnresolvedColumn col : schema.getColumns()) {
      UnresolvedPhysicalColumn column = (UnresolvedPhysicalColumn) col;
      if (!(column.getDataType() instanceof AtomicDataType)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public SchemaOperation visitDistinctAssignment(DistinctAssignment node, PlannerScope context) {
    SqlValidator validator = calcitePlanner.getValidator();
    SqlNode validated = validator.validate(context.getSqlNode());
    SqlToRelConverter sqlToRelConverter = calcitePlanner.getSqlToRelConverter(validator);

    RelNode relNode = sqlToRelConverter.convertQuery(validated, false, true).rel;
    DistinctScope distinctScope = (DistinctScope)context.getScope().getScopes().get(node);

    RelNode expanded = relNode.accept(new RelShuttleImpl(){
        @Override
        public RelNode visit(TableScan scan) {
          return distinctScope.getTable().getRelNode();
        }
      });

    Set<Integer> primaryKeys = new HashSet<>();
    for (Field field : distinctScope.getPartitionKeys()) {
      int index = getIndex(expanded.getRowType(), field.getName().getCanonical());
      primaryKeys.add(index);
    }

    List<Name> fields = distinctScope.getTable()
        .getFields().stream()
        .filter(f->f instanceof Column)
        .map(Field::getName)
        .collect(Collectors.toList());

    System.out.println(expanded.explain());
    return new AddQueryOp(node.getNamePath(), expanded, fields, primaryKeys, Set.of());
  }

  @Override
  public SchemaOperation visitExpressionAssignment(ExpressionAssignment node,
      PlannerScope context) {
    RelNode plan = this.calcitePlanner.plan(context.getSqlNode());

    Name versionedName = getShadowedName(context.getScope(), node.getNamePath().getLast());
    Integer index = getIndex(plan.getRowType(), versionedName.getCanonical());

    RelNode expanded = plan.accept(new ViewExpander(this.calcitePlanner));

    Set<Integer> primaryKeys = remapPrimary(context.getScope().getContextTable().get(), expanded);
    Set<Integer> parentPrimary = remapParentPrimary(context.getScope().getContextTable().get(), expanded);

    return new AddColumnOp(node.getNamePath().popLast(), node.getNamePath().getLast(), expanded, index, primaryKeys,
        parentPrimary);
  }

  private Set<Integer> remapPrimary(Table table, RelNode expanded) {
    return table.getPrimaryKey().stream()
        .map(i->table.getRelNode().getRowType().getFieldList().get(i))
        .map(f->getIndex(expanded.getRowType(), f.getName()))
        .collect(Collectors.toSet());
  }

  private Set<Integer> remapParentPrimary(Table table, RelNode expanded) {
    return table.getParentPrimaryKey().stream()
        .map(i->table.getRelNode().getRowType().getFieldList().get(i))
        .map(f->getIndex(expanded.getRowType(), f.getName()))
        .collect(Collectors.toSet());
  }

  public static Name getShadowedName(StatementScope scope, Name name) {
    if (scope.getContextTable().get().getField(name) != null) {
      int version = scope.getContextTable().get().getField(name).getVersion() + 1;
      return VersionedName.of(name, version);
    }

    return name;
  }
}
