package ai.datasqrl.plan.calcite.rules;

import ai.datasqrl.plan.calcite.table.ImportedSourceTable;
import ai.datasqrl.plan.calcite.table.QueryRelationalTable;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import ai.datasqrl.plan.calcite.util.SqrlRexUtil;
import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
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
        if (baseTable.getMatStrategy().isMaterialize()) {
          if (baseTable.getMatStrategy().isPullup()) {
            //TODO: inline database pullups into relNode
            throw new UnsupportedOperationException("Not yet implemented");
          } else {
            //Nothing to do since we table is materialized
          }
        } else {
          Preconditions.checkArgument(dbTable.isRoot() && !CalciteUtil.isNestedTable(baseTable.getRowType()));
          call.transformTo(baseTable.getRelNode());
        }
      }
      if (queryTable!=null) {
        Preconditions.checkArgument(!CalciteUtil.isNestedTable(queryTable.getRowType()));
        if (queryTable.getMatStrategy().isMaterialize()) {
          Preconditions.checkArgument(!StringUtils.isNullOrEmpty(queryTable.getMatStrategy().getPersistedAs()));
          RelBuilder builder = getBuilder(table);
          call.transformTo(builder.scan(queryTable.getMatStrategy().getPersistedAs()).build());
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
      VirtualRelationalTable dbTable = table.getTable()
              .unwrap(VirtualRelationalTable.class);
      QueryRelationalTable queryTable = table.getTable().unwrap(QueryRelationalTable.class);
      ImportedSourceTable sourceTable = table.getTable().unwrap(ImportedSourceTable.class);
      Preconditions.checkArgument(dbTable!=null ^ queryTable!=null ^ sourceTable!=null);
      if (dbTable!=null) {
        //Need to shred
        SQRLLogicalPlanConverter sqrl2sql = new SQRLLogicalPlanConverter(
                () -> getBuilder(table), new SqrlRexUtil(getBuilder(table).getRexBuilder()));
        RelNode expanded = table.accept(sqrl2sql);
        expanded = sqrl2sql.putPrimaryKeysUpfront(sqrl2sql.getRelHolder(expanded)).getRelNode();
        call.transformTo(expanded);
      }
      if (queryTable!=null) {
        if (queryTable.getMatStrategy().isPullup()) {
          call.transformTo(queryTable.getDbPullups().getBaseRelnode());
        } else {
          call.transformTo(queryTable.getRelNode());
        }
      }
      if (sourceTable != null) {
        //Leave as is
      }
    }

  }

}
