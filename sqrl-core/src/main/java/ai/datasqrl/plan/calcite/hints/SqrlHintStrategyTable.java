package ai.datasqrl.plan.calcite.hints;

import lombok.Getter;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategyTable;

public class SqrlHintStrategyTable {

  public static final String NOOP = "NOOP";

  public static final SqrlHint INTERVAL_JOIN = SqrlHint.of("IntervalJoin");

  @Getter
  static HintStrategyTable hintStrategyTable = HintStrategyTable.builder()
      .hintStrategy(TopNHint.Type.DISTINCT_ON.name(), HintPredicates.PROJECT)
      .hintStrategy(TopNHint.Type.SELECT_DISTINCT.name(), HintPredicates.PROJECT)
      .hintStrategy(TopNHint.Type.TOP_N.name(), HintPredicates.PROJECT)
      .hintStrategy(NOOP, HintPredicates.PROJECT)
      .hintStrategy(WatermarkHint.HINT_NAME, HintPredicates.or(HintPredicates.PROJECT,HintPredicates.TABLE_SCAN))
      .hintStrategy(JoinCostHint.HINT_NAME, HintPredicates.JOIN)
      .build();

}
