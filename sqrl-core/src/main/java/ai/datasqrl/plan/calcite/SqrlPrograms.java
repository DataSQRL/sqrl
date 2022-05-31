package ai.datasqrl.plan.calcite;

import static ai.datasqrl.plan.calcite.SqrlRuleSets.bindableRuleSet;
import static ai.datasqrl.plan.calcite.SqrlRuleSets.subqueryRuleSet;

import java.util.List;
import org.apache.calcite.interpreter.BindableConvention;

public class SqrlPrograms {

  public static HepSqrlProgram subquery = new HepSqrlProgram(subqueryRuleSet);

  public static VolcanoSqrlProgram bind = new VolcanoSqrlProgram(bindableRuleSet,
      BindableConvention.INSTANCE);

  public static SqrlChainProgram testProgram = new SqrlChainProgram(List.of(
      subquery,
      bind));
}
