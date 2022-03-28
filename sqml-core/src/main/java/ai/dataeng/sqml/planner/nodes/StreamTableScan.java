package ai.dataeng.sqml.planner.nodes;

import ai.dataeng.sqml.parser.operator.ImportManager.SourceTableImport;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;

@Getter
public class StreamTableScan extends TableScan {

  private final SourceTableImport tableImport;

  public StreamTableScan(RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      SourceTableImport tableImport) {
    super(cluster, traitSet, hints, table);
    this.tableImport = tableImport;
  }
}
