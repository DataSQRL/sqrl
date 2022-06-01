package ai.datasqrl.plan.calcite;

import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public class SqrlRuleSets {
  public static RuleSet bindableRuleSet = RuleSets.ofList(Bindables.RULES);

  public static RuleSet subqueryRuleSet = RuleSets.ofList(
      CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
      CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
      CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);
}
