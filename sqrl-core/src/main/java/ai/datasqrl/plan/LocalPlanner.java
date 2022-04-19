package ai.datasqrl.plan;

import static ai.datasqrl.parse.tree.name.Name.INGEST_TIME;

import ai.datasqrl.execute.StreamEngine;
import ai.datasqrl.execute.flink.environment.FlinkStreamEngine;
import ai.datasqrl.execute.flink.ingest.schema.FlinkTableConverter;
import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.ImportDefinition;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.calcite.CalcitePlanner;
import ai.datasqrl.plan.util.FlinkRelDataTypeConverter;
import ai.datasqrl.plan.util.FlinkSchemaUtil;
import ai.datasqrl.validate.scopes.ImportScope;
import ai.datasqrl.validate.scopes.StatementScope;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.planner.calcite.FlinkRelOptClusterFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWatermarkAssigner;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;

public class LocalPlanner extends AstVisitor<LocalPlannerResult, StatementScope> {
  private final FlinkTableConverter tbConverter = new FlinkTableConverter();
  private final CalcitePlanner calcitePlanner = new CalcitePlanner();

  public LocalPlannerResult plan(Node sqlNode, StatementScope scope) {
    return sqlNode.accept(this, scope);
  }

  /**
   * Import
   */
  @Override
  public LocalPlannerResult visitImportDefinition(ImportDefinition node, StatementScope scope) {
    ImportScope importScope = (ImportScope) scope.getScopes().get(node);

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

    ImportTable table = new ImportTable(tableName.toNamePath(), relNode, FlinkSchemaUtil.getFieldNames(schema));

    importedPaths.add(table);

    if (requiresShredding(schema)) {
      importedPaths.addAll(shred(tableName, streamRelType, schema));
    }

    return new ImportLocalPlannerResult(importedPaths);
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
    int indexOfField = 3;
    RelDataType t = FlinkTypeFactory.INSTANCE().createSqlType(SqlTypeName.INTEGER);

    RelBuilder b = builder
        .scanStream(baseStream, streamRelType)
//          .watermark(getIndex(streamRelType, INGEST_TIME.getCanonical())) TODO: FIX: timestamp is of the watermark type but an additional watermark gives wrong results (?)
        .values(List.of(List.of(rexBuilder.makeExactLiteral(BigDecimal.ZERO))),
            new RelRecordType(List.of(new RelDataTypeFieldImpl("ZERO", 0, t))))
        .project(List.of(builder.getRexBuilder().makeFieldAccess(
          rexBuilder.makeCorrel(streamRelType, id), fieldName, false
        )), List.of(fieldName))
        .uncollect(List.of(), false)
        .correlate(JoinRelType.INNER, id, RexInputRef.of(indexOfField, builder.peek().getRowType()))
        .project(projectShreddedColumns(rexBuilder, builder.peek()))//TODO: location of columns
        ;

    RelNode node = b.build();
    System.out.println(node.explain());

    List<Name> names = fields.stream()
        .map(f->Name.system(f.getName()))
        .collect(Collectors.toList());

    String columnName = fieldName;
    NamePath namePath = baseStream.toNamePath().concat(Name.system(columnName));
    ImportTable importTable = new ImportTable(namePath,
        node, names
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
}
