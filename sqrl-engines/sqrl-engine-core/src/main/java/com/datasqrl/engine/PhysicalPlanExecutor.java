/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine;

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.google.common.collect.Lists;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
@Slf4j
public class PhysicalPlanExecutor {

  private final Predicate<ExecutionStage> stageFilter;

  public PhysicalPlanExecutor() {
    this(s -> true);
  }

  public Result execute(PhysicalPlan physicalPlan, ErrorCollector errors) {
    List<StageResult> results = new ArrayList<>();
    //Need to execute plans backwards so all subsequent stages are ready before stage executes
    for (PhysicalPlan.StagePlan stagePlan : Lists.reverse(physicalPlan.getStagePlans())) {
      if (!stageFilter.test(stagePlan.getStage())) {
        log.info("Skipped stage: {}", stagePlan.getStage().getName());
      }
      log.info("Executing stage: {}", stagePlan.getStage().getName());
      results.add(
          new StageResult(stagePlan.getStage(), stagePlan.getStage().getEngine().execute(stagePlan.getPlan(), errors)));
    }
    return new Result(Lists.reverse(results));
  }

  @Value
  public static class Result {

    List<StageResult> results;

    public CompletableFuture get() {
      CompletableFuture[] futures = results.stream()
          .map(StageResult::getResult)
          .toArray(CompletableFuture[]::new);
      return CompletableFuture.allOf(futures);
    }
  }

  @Value
  public static class StageResult {

    ExecutionStage stage;
    CompletableFuture<ExecutionResult> result;

  }
}
