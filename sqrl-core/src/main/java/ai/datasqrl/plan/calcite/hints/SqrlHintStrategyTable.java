package ai.datasqrl.plan.calcite.hints;

import lombok.Getter;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategyTable;

public class SqrlHintStrategyTable {

  public static final String DISTINCT_ON = "DISTINCT_ON";

  public static final String SELECT_DISTINCT = "SELECT_DISTINCT";
  @Getter
  static HintStrategyTable hintStrategyTable = HintStrategyTable.builder()
      .hintStrategy(DISTINCT_ON, HintPredicates.PROJECT)
      .build();

}
