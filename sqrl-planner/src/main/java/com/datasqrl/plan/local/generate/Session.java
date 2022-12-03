package com.datasqrl.plan.local.generate;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.physical.pipeline.ExecutionPipeline;
import com.datasqrl.plan.calcite.Planner;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class Session {

  ErrorCollector errors;
  Planner planner;
  ExecutionPipeline pipeline;

}