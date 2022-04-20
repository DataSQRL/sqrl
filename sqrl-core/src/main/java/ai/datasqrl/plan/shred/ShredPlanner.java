package ai.datasqrl.plan.shred;

import static ai.datasqrl.parse.tree.name.Name.INGEST_TIME;
import static ai.datasqrl.plan.util.FlinkSchemaUtil.getIndex;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.operations.ImportTable;
import ai.datasqrl.plan.nodes.SqrlRelBuilder;
import ai.datasqrl.plan.calcite.CalcitePlanner;
import ai.datasqrl.plan.util.FlinkSchemaUtil;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.type.RelDataType;
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
  CalcitePlanner calcitePlanner;
  public List<ImportTable> shred(Name baseStream, RelDataType streamRelType, Schema schema) {
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
        tables.add(shred(baseStream, unresolvedPhysicalColumn.getName(), fieldsDataType, streamRelType));
      }
    }
    return tables;
  }

  private ImportTable shred(Name baseStream, String fieldName, FieldsDataType type,
      RelDataType streamRelType) {
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

    String columnName = fieldName;
    NamePath namePath = baseStream.toNamePath().concat(Name.system(columnName));
    ImportTable importTable = new ImportTable(namePath,
        node, FlinkSchemaUtil.getFieldNames(node),
        Set.of(getIndex(node.getRowType(), "_idx")),
        Set.of(0) //first index for uuid
    );

    return importTable;
  }

  public List<RexNode> projectShreddedColumns(RexBuilder rexBuilder,
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

}
