package ai.datasqrl.plan.calcite.rules;

import ai.datasqrl.plan.calcite.table.ImportedSourceTable;
import ai.datasqrl.plan.calcite.table.PullupOperator;
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
      LogicalTableScan table = call.rel(0);
      VirtualRelationalTable dbTable = table.getTable()
              .unwrap(VirtualRelationalTable.class);
      QueryRelationalTable queryTable = table.getTable().unwrap(QueryRelationalTable.class);
      Preconditions.checkArgument(dbTable!=null ^ queryTable!=null);
      if (dbTable!=null) {
        QueryRelationalTable baseTable = dbTable.getRoot().getBase();
        MaterializationStrategy strategy = baseTable.getMaterialization();
        if (strategy.isMaterialize()) {
          PullupOperator.Container pullup = dbTable.getDbPullups();
          RelBuilder relBuilder = getBuilder(table);
          relBuilder.push(table);
          if (!pullup.getNowFilter().isEmpty()) {
            //TODO: implement as TTL on table
            pullup.getNowFilter().addFilter(relBuilder);
          }
          if (!pullup.getDeduplication().isEmpty()) {
            //This is taken care of by UPSERTING against the primary key
          }
          call.transformTo(relBuilder.build());
        } else {
          Preconditions.checkArgument(dbTable.isRoot() && !CalciteUtil.isNestedTable(baseTable.getRowType()));
          call.transformTo(baseTable.getRelNode());
        }
      }
      if (queryTable!=null) {
        Preconditions.checkArgument(!CalciteUtil.isNestedTable(queryTable.getRowType()));
        MaterializationStrategy strategy = queryTable.getMaterialization();
        if (strategy.isMaterialize()) {
          Preconditions.checkArgument(!StringUtils.isNullOrEmpty(strategy.getPersistedAs()));
          RelBuilder builder = getBuilder(table);
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
