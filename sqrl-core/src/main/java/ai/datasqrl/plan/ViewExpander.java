package ai.datasqrl.plan;

import ai.datasqrl.plan.calcite.CalcitePlanner;
import ai.datasqrl.plan.nodes.SqrlViewTable;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;

@AllArgsConstructor
public class ViewExpander extends RelShuttleImpl {
  CalcitePlanner planner;

  @Override
  public RelNode visit(TableScan scan) {
    if (scan.getTable().getQualifiedName().get(0).equalsIgnoreCase("default_catalog")) {
      return super.visit(scan);
    }

    org.apache.calcite.schema.Table table = planner.getSchema()
        .getTable(scan.getTable().getQualifiedName().get(scan.getTable().getQualifiedName().size()-1), false).getTable();

    if (table instanceof SqrlViewTable) {
      return (((SqrlViewTable) table).getRelNode()).accept(this);
    } else if (!scan.getTable().getQualifiedName().get(0).endsWith("_stream")){
      //Replace with stream data type so calcite doesn't include columns that don't exist yet.
      RelOptTable table2 =
          planner.createRelBuilder().getRelOptSchema().getTableForMember(
              List.of(scan.getTable().getQualifiedName().get(0) + "_stream"));
      return new LogicalTableScan(scan.getCluster(), scan.getTraitSet(), scan.getHints(), table2);
    } else {
      return scan;
    }
  }
}