package ai.datasqrl.plan.local.shred;

import static ai.datasqrl.parse.tree.name.Name.INGEST_TIME;
import static ai.datasqrl.plan.util.FlinkSchemaUtil.getIndex;

import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.nodes.SqrlRelBuilder;
import ai.datasqrl.plan.util.FlinkRelDataTypeConverter;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.factory.TableFactory;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.FieldsDataType;

@AllArgsConstructor
public class ShredPlanner {
  public RelNode plan(Name tableName, SqrlRelBuilder builder,
      org.apache.flink.table.api.Schema schema, Table table) {

    RelNode relNode = builder
        .scanStream(tableName, table)
        .watermark(getIndex(schema, INGEST_TIME.getCanonical()))
        .project(FlinkRelDataTypeConverter.getScalarIndexes(schema))
        .build();
    return relNode;
  }

  public RelNode planNested(SqrlRelBuilder builder, Name baseStream, String fieldName, Table parentTable) {
    RexBuilder rexBuilder = builder.getRexBuilder();
    CorrelationId id = new CorrelationId(0);
    int indexOfField = getIndex(parentTable.getRowType(), fieldName);
    RelDataType t = FlinkTypeFactory.INSTANCE().createSqlType(SqlTypeName.INTEGER);
    RelBuilder b = builder
        .scanStream(baseStream, parentTable)
        .watermark(getIndex(parentTable.getRowType(), INGEST_TIME.getCanonical()))
        .values(List.of(List.of(rexBuilder.makeExactLiteral(BigDecimal.ZERO))),
            new RelRecordType(List.of(new RelDataTypeFieldImpl("ZERO", 0, t))))
        .project(List.of(builder.getRexBuilder().makeFieldAccess(
            rexBuilder.makeCorrel(parentTable.getRowType(), id), fieldName, false
        )), List.of(fieldName))
        .uncollect(List.of(), false)
        .correlate(JoinRelType.INNER, id, RexInputRef.of(indexOfField, builder.peek().getRowType()))
        .project(projectShreddedColumns(rexBuilder, builder.peek()));//, fieldNames(builder.peek()));
    RelNode node = b.build();
    return node;
  }
//
//  private List<String> fieldNames(RelNode node) {
//    List<String> names = new ArrayList<>();
//    names.add("__pk_0_uuid");
//    LogicalCorrelate correlate = (LogicalCorrelate) node;
//
//    //All columns on rhs
//    for (int i = correlate.getLeft().getRowType().getFieldCount();
//        i < correlate.getRowType().getFieldCount(); i++) {
//      if (correlate.getRowType().getFieldNames().get(i).equalsIgnoreCase("_idx")) {
//        names.add("__pk_1_idx");
//      } else {
//        names.add(correlate.getRowType().getFieldNames().get(i));
//      }
//    }
//    return names;
//  }

  public List<RexNode> projectShreddedColumns(RexBuilder rexBuilder,
      RelNode node) {
    List<RexNode> projects = new ArrayList<>();
    LogicalCorrelate correlate = (LogicalCorrelate) node;
    for (int i = 0; i < correlate.getLeft().getRowType().getFieldCount(); i++) {
      String name = correlate.getLeft().getRowType().getFieldNames().get(i);
      if (name.equalsIgnoreCase(Name.UUID.getCanonical())) {//|| name.equalsIgnoreCase("_ingest_time")) {
        projects.add(rexBuilder.makeInputRef(node, i));
      }
    }

    //All columns on rhs
    for (int i = correlate.getLeft().getRowType().getFieldCount();
        i < correlate.getRowType().getFieldCount(); i++) {
      projects.add(rexBuilder.makeInputRef(node, i));
    }

    return projects;
  }

  public void shred(Name baseStream, Schema schema, Table table, SourceTableStatistics sourceStats,
                    SqrlRelBuilder builder) {
    List<UnresolvedColumn> columns = schema.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      UnresolvedColumn col = columns.get(i);
      UnresolvedPhysicalColumn unresolvedPhysicalColumn = (UnresolvedPhysicalColumn) col;
      if (unresolvedPhysicalColumn.getDataType() instanceof FieldsDataType
          || unresolvedPhysicalColumn.getDataType() instanceof CollectionDataType) {
        String nestedTableName = unresolvedPhysicalColumn.getName();
        shred(baseStream, nestedTableName, table, sourceStats, builder);
      }
    }
  }

  private void shred(Name baseStream, String fieldName, Table parentTable,
                  SourceTableStatistics sourceStats, SqrlRelBuilder builder) {
    RelNode relNode = planNested(builder, baseStream, fieldName, parentTable);

    List<Column> columns = relNode.getRowType().getFieldList().stream()
        .map(f->createNestedColumn(f, parentTable.getPrimaryKeys()))
            .collect(Collectors.toList());

    NamePath tableName = baseStream.toNamePath().concat(Name.system(fieldName));

    TableFactory tableFactory = new TableFactory();

    Table table = tableFactory.createSourceTable(tableName, columns, sourceStats.getRelationStats(tableName));
    tableFactory.assignRelationships(Name.system(fieldName), table, parentTable);
    table.setHead(relNode);
  }

  private Column createNestedColumn(RelDataTypeField field, List<Column> parentPrimaryKeys) {
    final Name fieldName = Name.system(field.getName()); //TODO: Need to preserve name casing
    boolean isParentPrimaryKey = parentPrimaryKeys.stream().map(Column::getName).anyMatch(n -> fieldName.equals(n));
    boolean isPrimaryKey = isParentPrimaryKey || fieldName.equals(ReservedName.ARRAY_IDX);

    //TODO: Need to put timestamp into child
    return new Column(fieldName, 0, field, List.of(), false,
        isPrimaryKey, isParentPrimaryKey, false);
  }
}
