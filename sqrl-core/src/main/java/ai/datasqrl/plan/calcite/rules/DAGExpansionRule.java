package ai.datasqrl.plan.calcite.rules;

import ai.datasqrl.plan.calcite.table.QueryRelationalTable;
import ai.datasqrl.plan.calcite.table.SourceTable;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.tools.RelBuilder;

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


  public static class ReadOnly extends DAGExpansionRule {

    @Override
    public void onMatch(RelOptRuleCall call) {
      LogicalTableScan scan = call.rel(0);
      VirtualRelationalTable vTable = scan.getTable()
              .unwrap(VirtualRelationalTable.class);
      Preconditions.checkArgument(vTable!=null);
      QueryRelationalTable queryTable = vTable.getRoot().getBase();
      if (queryTable.getExecution().isRead()) {
        Preconditions.checkArgument(!CalciteUtil.hasNesting(queryTable.getRowType()));
        call.transformTo(queryTable.getRelNode());
      }
    }

  }

  public static class Read2Write extends DAGExpansionRule {

    @Override
    public void onMatch(RelOptRuleCall call) {
      LogicalTableScan scan = call.rel(0);
      VirtualRelationalTable vTable = scan.getTable()
              .unwrap(VirtualRelationalTable.class);
      Preconditions.checkArgument(vTable!=null);
      QueryRelationalTable queryTable = vTable.getRoot().getBase();
      Preconditions.checkArgument(queryTable.getExecution().isWrite());
      if (!vTable.isRoot() && vTable.getRoot().getBase().getTimestamp().hasFixedTimestamp()) {
        RelBuilder relBuilder = getBuilder(scan);
        relBuilder.scan(vTable.getNameId()); //Update scan since we changed tables in DAG planner to add columns
        CalciteUtil.addIdentityProjection(relBuilder, relBuilder.peek().getRowType().getFieldCount() - 1);
        call.transformTo(relBuilder.build());
      } //else do nothing; the table is materialized and can be consumed without adjustment

    }

  }


  public static class WriteOnly extends DAGExpansionRule {

    @Override
    public void onMatch(RelOptRuleCall call) {
      final LogicalTableScan table = call.rel(0);
      QueryRelationalTable queryTable = table.getTable().unwrap(QueryRelationalTable.class);
      SourceTable sourceTable = table.getTable().unwrap(SourceTable.class);
      Preconditions.checkArgument(queryTable!=null ^ sourceTable!=null);
      if (queryTable!=null) {
        RelBuilder relBuilder = getBuilder(table);
        relBuilder.push(queryTable.getRelNode());
        //We might have added additional columns to table so restrict to length of original rowtype
        CalciteUtil.addIdentityProjection(relBuilder,table.getRowType().getFieldCount());
        call.transformTo(relBuilder.build());
      }
      if (sourceTable != null) {
        //Leave as is
      }
    }

  }

}
