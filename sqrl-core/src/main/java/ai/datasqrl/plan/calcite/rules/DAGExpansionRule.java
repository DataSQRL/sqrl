package ai.datasqrl.plan.calcite.rules;

import ai.datasqrl.plan.calcite.table.ImportedSourceTable;
import ai.datasqrl.plan.calcite.table.QueryRelationalTable;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import ai.datasqrl.plan.global.MaterializationStrategy;
import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.tools.RelBuilder;
import org.h2.util.StringUtils;

/**
 *
 */
public abstract class DAGExpansionRule extends RelOptRule {

  public DAGExpansionRule() {
    super(operand(LogicalTableScan.class, any()));

  }

  public RelBuilder getBuilder(LogicalTableScan table) {
    return relBuilderFactory.create(table.getCluster(), table.getTable().getRelOptSchema());
  }


  public static class Read extends DAGExpansionRule {

    @Override
    public void onMatch(RelOptRuleCall call) {
      LogicalTableScan scan = call.rel(0);
      VirtualRelationalTable dbTable = scan.getTable()
              .unwrap(VirtualRelationalTable.class);
      QueryRelationalTable queryTable = scan.getTable().unwrap(QueryRelationalTable.class);
      Preconditions.checkArgument(dbTable!=null ^ queryTable!=null);
      if (dbTable!=null) {
        //Do nothing
      }
      if (queryTable!=null) {
        Preconditions.checkArgument(!CalciteUtil.isNestedTable(queryTable.getRowType()));
        MaterializationStrategy strategy = queryTable.getMaterialization();
        if (strategy.isMaterialize()) {
          Preconditions.checkArgument(!StringUtils.isNullOrEmpty(strategy.getPersistedAs()));
          RelBuilder builder = getBuilder(scan);
          call.transformTo(builder.scan(strategy.getPersistedAs()).build());
        } else {
          call.transformTo(queryTable.getRelNode());
        }
      }
    }

  }


  public static class Write extends DAGExpansionRule {

    @Override
    public void onMatch(RelOptRuleCall call) {
      final LogicalTableScan table = call.rel(0);
      QueryRelationalTable queryTable = table.getTable().unwrap(QueryRelationalTable.class);
      ImportedSourceTable sourceTable = table.getTable().unwrap(ImportedSourceTable.class);
      Preconditions.checkArgument(queryTable!=null ^ sourceTable!=null);
      if (queryTable!=null) {
        call.transformTo(queryTable.getRelNode());
      }
      if (sourceTable != null) {
        //Leave as is
      }
    }

  }

}
