package ai.datasqrl.plan.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;

/**
 * Process flow:
 *
 * Convert Transpiled sqrl to SqlNode
 * Validate SqlNode
 * Convert to rel
 * Execute planner rules via 'transform' method
 *   (Planner rules ordered and indexed in Rules class)
 */
public class Planner extends PlannerImpl {

  public Planner(FrameworkConfig config) {
    super(config);
    ready();
  }

  public void refresh() {
    close();
    reset();
    ready();
  }

  public RelBuilder getRelBuilder() {
    RelOptCluster cluster = RelOptCluster.create(this.planner, createRexBuilder());
    return sqlToRelConverterConfig.getRelBuilderFactory().create(cluster, createCatalogReader());
  }

  public SqrlType2Calcite getTypeConverter() {
    return new SqrlType2Calcite(typeFactory);
  }

  public RelNode transform(OptimizationStage stage, RelNode node) {
    RelTraitSet outputTraits = getEmptyTraitSet();
    outputTraits = stage.getTrait().map(outputTraits::replace).orElse(outputTraits);
    return super.transform(stage.getIndex(), outputTraits, node);
  }

}