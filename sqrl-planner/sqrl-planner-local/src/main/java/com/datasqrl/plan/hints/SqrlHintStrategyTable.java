/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.hints;

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
      .hintStrategy(WatermarkHint.HINT_NAME,
          HintPredicates.or(HintPredicates.PROJECT, HintPredicates.TABLE_SCAN))
      .hintStrategy(JoinCostHint.HINT_NAME, HintPredicates.JOIN)
      .hintStrategy(SlidingAggregationHint.HINT_NAME, HintPredicates.AGGREGATE)
      .hintStrategy(TumbleAggregationHint.HINT_NAME, HintPredicates.AGGREGATE)
      .hintStrategy(TemporalJoinHint.HINT_NAME, HintPredicates.JOIN)
      .hintStrategy(DedupHint.HINT_NAME, HintPredicates.JOIN)
      .hintStrategy(INTERVAL_JOIN.getHintName(), HintPredicates.JOIN)
      .build();

}
