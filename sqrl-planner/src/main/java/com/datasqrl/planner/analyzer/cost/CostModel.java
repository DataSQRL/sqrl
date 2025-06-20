package com.datasqrl.planner.analyzer.cost;

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.planner.analyzer.TableAnalysis;

public interface CostModel {

  ComputeCost getSourceSinkCost();

  ComputeCost getCost(ExecutionStage executionStage, TableAnalysis tableAnalysis);
}
