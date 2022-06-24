package ai.datasqrl.plan.calcite.sqrl.rules;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.calcite.sqrl.table.LogicalBaseTableCalciteTable;
import ai.datasqrl.plan.calcite.sqrl.table.QueryCalciteTable;
import ai.datasqrl.plan.calcite.sqrl.table.SourceTableCalciteTable;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.tools.RelBuilder;

/**
 *
 */
public class SqrlExpansionRelRule extends RelOptRule {

  public SqrlExpansionRelRule() {
    super(operand(LogicalTableScan.class, any()));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalTableScan table = call.rel(0);
    LogicalBaseTableCalciteTable baseTable = table.getTable()
        .unwrap(LogicalBaseTableCalciteTable.class);
    SourceTableCalciteTable sourceTable = table.getTable()
        .unwrap(SourceTableCalciteTable.class);
    QueryCalciteTable query = table.getTable().unwrap(QueryCalciteTable.class);

    /*
     * A dataset table, such as ecommerce-data.Products
     */
    if (sourceTable != null) {
      RelOptTable sourceTableRelOptTable = table.getTable().getRelOptSchema()
          .getTableForMember(List.of(sourceTable.getSourceTableImport().getTable().qualifiedName()));

      RelNode scan = LogicalTableScan.create(table.getCluster(), sourceTableRelOptTable,
          table.getHints());

      call.transformTo(scan);
    } else if (baseTable != null) { //A logical table that may need shredding, such as Products$1
      RelOptTable baseDatasetRelOptTable = table.getTable().getRelOptSchema()
          .getTableForMember(List.of(baseTable.getSourceTableImport().getTable().qualifiedName()));

      RelNode relNode = shred(baseTable.getShredPath(), baseTable.getSourceTableImport(),
          baseDatasetRelOptTable, table);

      call.transformTo(relNode);

    } else if (query != null) {//A query backed table
      call.transformTo(query.getRelNode());
    } else {
      throw new RuntimeException("Unknown table type :" + table.getTable().getClass().getName());
    }
//    RelBuilder builder = this.relBuilderFactory.create(table.getCluster(), table.getTable().getRelOptSchema());
//
//    RexBuilder rexBuilder = builder.getRexBuilder();
//    CorrelationId id = new CorrelationId(0);
//    int indexOfField = getIndex(sourceTable.getRootType(), sourceTable.getName().getLast()
//    .getCanonical());
//
//    builder.scan(sourceTable.getRootTable().getId().getCanonical())
//        .values(List.of(List.of(builder.getRexBuilder().makeExactLiteral(BigDecimal.ZERO))),
//            new RelRecordType(List.of(new RelDataTypeFieldImpl(
//                "ZERO",
//                0,
//                builder.getTypeFactory().createSqlType(SqlTypeName.INTEGER)))))
//        .project(
//            List.of(builder.getRexBuilder()
//                .makeFieldAccess(
//                    builder.getRexBuilder().makeCorrel(sourceTable.getRootType(), id),
//                    sourceTable.getName().getLast().getCanonical(),
//                    false)),
//            List.of(sourceTable.getName().getLast().getCanonical()))
//        .uncollect(List.of(), false)
//        .correlate(JoinRelType.INNER, id, RexInputRef.of(indexOfField, builder.peek()
//        .getRowType()))
//        .project(projectShreddedColumns(rexBuilder, builder.peek()));//, fieldNames(builder
//        .peek()));

//      call.transformTo(builder.build());
  }

  private RelNode shred(NamePath shredPath, SourceTableImport sourceTableImport,
      RelOptTable baseDatasetRelOptTable, LogicalTableScan table) {

    RelNode scan = LogicalTableScan.create(table.getCluster(), baseDatasetRelOptTable,
        table.getHints());

    if (shredPath.getLength() == 1) {
      RelBuilder builder = relBuilderFactory.create(table.getCluster(), table.getTable().getRelOptSchema());

      RelDataTypeFactory.FieldInfoBuilder fieldBuilder = table.getTable().getRelOptSchema().getTypeFactory()
          .builder().kind(StructKind.FULLY_QUALIFIED);
      for (RelDataTypeField field : table.getRowType().getFieldList()) {
        if (field.getType() instanceof BasicSqlType && !(field.getType() instanceof ArraySqlType)) {
          fieldBuilder.add(field.getName(), field.getType());
        }
      }

      RelDataType type = fieldBuilder.build();

      if (type.getFieldList().size() == scan.getRowType().getFieldList().size()) {
        return scan;
      } else {
        return builder.build();
      }
    } else if (shredPath.getLength() ==2) {
//
//        RelBuilder builder = relBuilderFactory.create(table.getCluster(),
//            table.getTable().getRelOptSchema());
//
//        RexBuilder rexBuilder = builder.getRexBuilder();
//        CorrelationId id = new CorrelationId(0);
//        int indexOfField = 5;//getIndex(sourceTable.getRootType(), sourceTable.getName().getLast()
////      .getCanonical());
//
//        builder.scan(sourceTableImport.getTable().qualifiedName());
//        builder
//            .values(List.of(List.of(builder.getRexBuilder().makeExactLiteral(BigDecimal.ZERO))),
//                new RelRecordType(List.of(new RelDataTypeFieldImpl(
//                    "ZERO",
//                    0,
//                    builder.getTypeFactory().createSqlType(SqlTypeName.INTEGER)))))
//            .project(
//                List.of(builder.getRexBuilder()
//                    .makeFieldAccess(
//                        builder.getRexBuilder().makeCorrel(<loigcalrowtype>, id),
//                        <shredfieldname>,
//                        false)),
//                List.of(<shredfieldname>))
//            .uncollect(List.of(), false)
//            .correlate(JoinRelType.INNER, id, RexInputRef.of(indexOfField, builder.peek()
//                .getRowType()))
//            .project(projectShreddedColumns(rexBuilder, builder.peek()));
//
//        scan = builder.build();
    }

    return scan;
  }

  public List<RexNode> projectShreddedColumns(RexBuilder rexBuilder,
      RelNode node) {
    List<RexNode> projects = new ArrayList<>();
    LogicalCorrelate correlate = (LogicalCorrelate) node;
    for (int i = 0; i < correlate.getLeft().getRowType().getFieldCount(); i++) {
      String name = correlate.getLeft().getRowType().getFieldNames().get(i);
      if (name.equalsIgnoreCase(ReservedName.UUID.getCanonical())) {//|| name.equalsIgnoreCase("_ingest_time")) {
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

  public static int getIndex(RelDataType type, String name) {
    for (int i = 0; i < type.getFieldList().size(); i++) {
      if (type.getFieldList().get(i).getName().equalsIgnoreCase(name)) {
        return i;
      }
    }

    return -1;
  }
}
