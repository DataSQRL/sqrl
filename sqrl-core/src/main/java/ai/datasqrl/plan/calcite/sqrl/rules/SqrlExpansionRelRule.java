package ai.datasqrl.plan.calcite.sqrl.rules;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.calcite.memory.table.DataTable;
import ai.datasqrl.plan.calcite.sqrl.table.ImportedSqrlTable;
import ai.datasqrl.plan.calcite.sqrl.table.QuerySqrlTable;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
    ImportedSqrlTable baseTable = table.getTable()
        .unwrap(ImportedSqrlTable.class);
    QuerySqrlTable query = table.getTable().unwrap(QuerySqrlTable.class);
    DataTable dt = table.getTable().unwrap(DataTable.class);

    /*
     * A dataset table, such as ecommerce-data.Products
     */
    if (baseTable != null) { //A logical table that may need shredding, such as Products$1
      RelOptTable baseDatasetRelOptTable = table.getTable().getRelOptSchema()
          .getTableForMember(List.of(baseTable.getSourceTableImport().getTable().qualifiedName()));

      RelNode relNode = null; /*shred(baseTable.getNamePath(), baseTable.getSourceTableImport(),
          baseDatasetRelOptTable, table); */

      call.transformTo(relNode);

    } else if (query != null) {//A query backed table
//      call.transformTo(query.getRelNode());
    } else {
//      throw new RuntimeException("Unknown table type :" + table.getTable().getClass().getName());
    }
  }

  private RelNode shred(NamePath shredPath, SourceTableImport sourceTableImport,
      RelOptTable baseDatasetRelOptTable, LogicalTableScan table) {

    RelNode scan = LogicalTableScan.create(table.getCluster(), baseDatasetRelOptTable,
        table.getHints());

    if (sourceTableImport.getTable().getName().equals(Name.system("orders"))) {
      if (shredPath.getLength() == 1) {
        RelBuilder builder = relBuilderFactory.create(table.getCluster(), table.getTable().getRelOptSchema());
        RexBuilder rexBuilder = builder.getRexBuilder();

        builder.push(scan)
            .project(IntStream.range(0, 5).mapToObj(i->rexBuilder.makeInputRef(builder.peek(), i))
                .collect(Collectors.toList()));
        return builder.build();
      } else {
        RelBuilder builder = relBuilderFactory.create(table.getCluster(), table.getTable().getRelOptSchema());
        RexBuilder rexBuilder = builder.getRexBuilder();
        CorrelationId id = new CorrelationId(0);
        int indexOfField = 4;//getIndex(sourceTable.getRootType(), sourceTable.getName().getLast().getCanonical());
        builder.scan(sourceTableImport.getTable().qualifiedName());
        RelDataType base = builder.peek().getRowType();
        builder
            .values(List.of(List.of(builder.getRexBuilder().makeExactLiteral(BigDecimal.ZERO))),
                new RelRecordType(List.of(new RelDataTypeFieldImpl(
                    "ZERO",
                    0,
                    builder.getTypeFactory().createSqlType(SqlTypeName.INTEGER)))))
            .project(
                List.of(builder.getRexBuilder()
                    .makeFieldAccess(
                        builder.getRexBuilder().makeCorrel(base, id),
                        "entries",
                        false)),
                List.of("entries"))
            .uncollect(List.of(), false)
            .correlate(JoinRelType.INNER, id, RexInputRef.of(indexOfField,  builder.peek().getRowType()))
            .project(projectShreddedColumns(rexBuilder, builder.peek()));

        return builder.build();
      }

    }
    if (shredPath.getLength() == 1) {
      RelBuilder builder = relBuilderFactory.create(table.getCluster(), table.getTable().getRelOptSchema());

      CalciteUtil.RelDataTypeBuilder fieldBuilder = CalciteUtil.getRelTypeBuilder(table.getTable().getRelOptSchema().getTypeFactory());

      for (RelDataTypeField field : table.getRowType().getFieldList()) {
        if (field.getType() instanceof BasicSqlType && !(field.getType() instanceof ArraySqlType)) {
          fieldBuilder.add(field);
        }
      }

      RelDataType type = fieldBuilder.build();

      if (type.getFieldList().size() == scan.getRowType().getFieldList().size()) {
        return scan;
      } else {
        return builder.build();
      }
    }

    return scan;
  }

  public List<RexNode> projectShreddedColumns(RexBuilder rexBuilder,
      RelNode node) {
    List<RexNode> projects = new ArrayList<>();
    LogicalCorrelate correlate = (LogicalCorrelate) node;
    for (int i = 0; i < correlate.getLeft().getRowType().getFieldCount(); i++) {
      String name = correlate.getLeft().getRowType().getFieldNames().get(i);
      if (name.equalsIgnoreCase(ReservedName.UUID.getCanonical()) || name.equalsIgnoreCase("_ingest_time")) {
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
