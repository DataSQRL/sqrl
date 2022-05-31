package ai.datasqrl.plan.calcite;

import java.util.List;
import lombok.Builder;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;

@Builder
public class VolcanoSqrlProgram extends SqrlProgram {
  RuleSet ruleset;

  RelTrait toTrait;

  @Override
  public RelNode apply(RelNode relNode) {
    RelOptPlanner planner = relNode.getCluster().getPlanner();
    planner.clear();

    RelTraitSet traits = relNode.getTraitSet().plus(toTrait).simplify();
    Program program = Programs.ofRules(ruleset);
    relNode = program.run(planner, relNode, traits, List.of(), List.of());

    return relNode;
  }
}
