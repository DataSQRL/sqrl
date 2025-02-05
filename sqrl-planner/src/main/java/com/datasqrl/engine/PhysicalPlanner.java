/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.engine.PhysicalPlan.PhysicalStagePlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.WriteQuery;
import com.datasqrl.util.StreamUtil;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_=@Inject)
public class PhysicalPlanner {
  SqrlFramework framework;
//  TableSink errorSink;
  ErrorCollector errorCollector;

  public PhysicalPlan plan(PhysicalDAGPlan plan) {
    List<PhysicalStagePlan> physicalStages = new ArrayList<>();

    for (int i = 0; i < plan.getStagePlans().size(); i++) {
      PhysicalDAGPlan.StagePlan stagePlan = plan.getStagePlans().get(i);
      //1. Get all queries that sink into this stage
      List<PhysicalDAGPlan.StageSink> inputs = StreamUtil.filterByClass(
              plan.getWriteQueries().stream().map(WriteQuery::getSink), PhysicalDAGPlan.StageSink.class)
          .filter(sink -> sink.getStage().equals(stagePlan.getStage()))
          .collect(Collectors.toList());

      EnginePhysicalPlan physicalPlan = stagePlan.getStage().getEngine().plan(stagePlan, inputs,
          plan.getPipeline(), plan.getStagePlans(), framework, errorCollector);

      physicalStages.add(new PhysicalStagePlan(stagePlan.getStage(), physicalPlan));
    }

    return new PhysicalPlan(physicalStages);
  }
}
