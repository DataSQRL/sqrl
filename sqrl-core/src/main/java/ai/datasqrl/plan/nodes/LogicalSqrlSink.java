package ai.datasqrl.plan.nodes;

import ai.datasqrl.schema.Table;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

@Getter
public class LogicalSqrlSink extends SingleRel {

  private final Table queryTable;

  public LogicalSqrlSink(RelOptCluster cluster, RelTraitSet traits, RelNode input,
      Table queryTable) {
    super(cluster, traits, input);
    this.queryTable = queryTable;
  }
}
