package ai.datasqrl.plan.calcite.hints;

import lombok.Getter;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategyTable;

public class SqrlHintStrategyTable {
  public static final String DISTINCT_ON_HINT_NAME = "DISTINCT_ON";
  @Getter
  static HintStrategyTable hintStrategyTable = HintStrategyTable.builder()
      .hintStrategy(DISTINCT_ON_HINT_NAME, HintPredicates.TABLE_SCAN)
      .build();

}
