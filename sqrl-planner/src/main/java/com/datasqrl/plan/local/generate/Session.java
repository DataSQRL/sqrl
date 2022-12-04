/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.local.generate;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
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