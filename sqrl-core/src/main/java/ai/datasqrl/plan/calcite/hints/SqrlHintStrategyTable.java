package ai.datasqrl.plan.calcite.hints;

import lombok.Getter;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategyTable;

public class SqrlHintStrategyTable {
  public static final String TOP_N = "TOP_N";
  @Getter
  static HintStrategyTable hintStrategyTable = HintStrategyTable.builder()
      .hintStrategy(TOP_N, HintPredicates.PROJECT)
      .build();

}
