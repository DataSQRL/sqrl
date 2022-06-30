package ai.datasqrl.plan.calcite;

import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.plan.local.transpiler.toSql.ConvertContext;
import ai.datasqrl.plan.local.transpiler.toSql.SqlNodeConverter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
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

  SqlNodeConverter sqlNodeConverter = new SqlNodeConverter();

  public Planner(FrameworkConfig config) {
    super(config);
    ready();
  }

  public void refresh() {
    close();
    reset();
    ready();
  }

  /**
   * Similar to calcite's parse except converts a sqrl node.
   */
  public SqlNode convert(Node node) {
    switch (state) {
      case STATE_0_CLOSED:
      case STATE_1_RESET:
        ready();
        break;
      default:
        break;
    }
    ensure(State.STATE_2_READY);
    SqlNode sqlNode = node.accept(sqlNodeConverter, new ConvertContext());
    state = State.STATE_3_PARSED;
    return sqlNode;
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