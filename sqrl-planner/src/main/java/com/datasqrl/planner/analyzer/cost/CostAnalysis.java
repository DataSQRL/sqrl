package com.datasqrl.planner.analyzer.cost;


/**
 * As we analyze a query, collect {@link CostAnalysis} as we encounter
 * expensive operations that are better to execute in certain engine types.
 *
 * These are then analyzed collectively in the cost analysis model.
 */
public interface CostAnalysis {

  double getCostMultiplier();

}
