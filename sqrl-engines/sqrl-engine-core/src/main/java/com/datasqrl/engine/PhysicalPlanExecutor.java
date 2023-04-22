/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine;

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
@Slf4j
public class PhysicalPlanExecutor {

  public Result execute(PhysicalPlan physicalPlan, ErrorCollector errors) {
    List<StageResult> results = new ArrayList<>();
    //Need to execute plans backwards so all subsequent stages are ready before stage executes
    for (PhysicalPlan.StagePlan stagePlan : Lists.reverse(physicalPlan.getStagePlans())) {
      results.add(
          new StageResult(stagePlan.getStage(), stagePlan.getStage().execute(stagePlan.getPlan(), errors)));
    }
    return new Result(Lists.reverse(results));
  }

  @Value
  public static class Result {

    List<StageResult> results;

  }

  @Value
  public static class StageResult {

    ExecutionStage stage;
    ExecutionResult result;

  }
}
