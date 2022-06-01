package ai.datasqrl.plan.calcite;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RuleSet;

@Builder
public class HepSqrlProgram extends SqrlProgram {
  RuleSet ruleset;

  @Override
  public RelNode apply(RelNode relNode) {
    HepProgram program = new HepProgramBuilder()
        .addRuleCollection(ImmutableList.copyOf(ruleset.iterator()))
        .build();
    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    return hepPlanner.findBestExp();
  }
}
