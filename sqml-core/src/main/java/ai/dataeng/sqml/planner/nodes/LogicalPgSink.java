package ai.dataeng.sqml.planner.nodes;

import ai.dataeng.sqml.parser.operator.ImportManager.SourceTableImport;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

public class LogicalPgSink extends SingleRel {

  public LogicalPgSink(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, traits, input);
  }
}
