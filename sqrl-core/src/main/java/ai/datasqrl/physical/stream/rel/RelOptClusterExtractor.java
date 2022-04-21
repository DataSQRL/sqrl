package ai.datasqrl.physical.stream.rel;

import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;

public class RelOptClusterExtractor extends RelShuttleImpl {

  @Getter
  RelOptCluster cluster = null;

  public static RelOptCluster extract(String tableName, StreamTableEnvironmentImpl tEnv) {
    PlannerQueryOperation op = (PlannerQueryOperation) tEnv.getPlanner().getParser()
        .parse("SELECT * FROM " + tableName).get(0);
    RelOptClusterExtractor clusterGetter = new RelOptClusterExtractor();
    op.getCalciteTree().accept(clusterGetter);

    return clusterGetter.getCluster();
  }

  @Override
  public RelNode visit(TableScan scan) {
    cluster = scan.getCluster();
    return super.visit(scan);
  }
}
