package ai.datasqrl.plan.nodes;

import ai.datasqrl.schema.Table;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

@Getter
@Setter
//TODO: This should be moved to physical.stream.flink.plan ??
public class LogicalFlinkSink extends SingleRel {

  private final Table sqrlTable;

  private String physicalName;

  public LogicalFlinkSink(RelOptCluster cluster, RelTraitSet traits, RelNode input,
      Table sqrlTable) {
    super(cluster, traits, input);
    this.sqrlTable = sqrlTable;
  }
}
