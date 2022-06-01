package ai.datasqrl.plan.calcite;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;

public class MultiphaseOptimizer {
  public RelNode optimize(RelNode relNode, SqrlChainProgram program) {
    for (SqrlProgram p : program.getPrograms()) {
      relNode = p.apply(relNode);
    }

    return relNode;
  }

  public static RelOptCluster newCluster(RelDataTypeFactory factory) {
    RelOptPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    return RelOptCluster.create(planner, new RexBuilder(factory));
  }
}
