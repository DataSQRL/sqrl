package ai.datasqrl.plan.calcite.sqrl.rules;

import ai.datasqrl.plan.calcite.sqrl.tables.DatasetTableCalciteTable;
import ai.datasqrl.plan.calcite.sqrl.tables.QueryCalciteTable;
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
    DatasetTableCalciteTable dataset = table.getTable().unwrap(DatasetTableCalciteTable.class);
    QueryCalciteTable query = table.getTable().unwrap(QueryCalciteTable.class);

    if (dataset != null) {
      RelOptTable baseDatasetRelOptTable = table.getTable().getRelOptSchema().getTableForMember(
          List.of(dataset.getSourceTableImport().getTable().qualifiedName()));

      RelNode scan = LogicalTableScan.create(table.getCluster(), baseDatasetRelOptTable, table.getHints());

      call.transformTo(scan);
    } else if (query != null) {
      call.transformTo(query.getRelNode());
    } else {

    }
  }
}
