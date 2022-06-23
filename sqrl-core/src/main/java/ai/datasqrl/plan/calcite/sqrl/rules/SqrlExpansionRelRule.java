package ai.datasqrl.plan.calcite.sqrl.rules;

import ai.datasqrl.plan.calcite.sqrl.table.LogicalBaseTableCalciteTable;
import ai.datasqrl.plan.calcite.sqrl.table.SourceTableCalciteTable;
import ai.datasqrl.plan.calcite.sqrl.table.QueryCalciteTable;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;

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
    LogicalBaseTableCalciteTable baseTable = table.getTable().unwrap(LogicalBaseTableCalciteTable.class);
    QueryCalciteTable query = table.getTable().unwrap(QueryCalciteTable.class);

    if (baseTable != null) {
      RelOptTable baseDatasetRelOptTable = table.getTable().getRelOptSchema()
          .getTableForMember(List.of(baseTable.getSourceTableImport().getTable().qualifiedName()));

      RelNode scan = LogicalTableScan.create(table.getCluster(), baseDatasetRelOptTable, table.getHints());

      call.transformTo(scan);
    } else if (query != null) {
      call.transformTo(query.getRelNode());
    } else {

    }
  }
}
