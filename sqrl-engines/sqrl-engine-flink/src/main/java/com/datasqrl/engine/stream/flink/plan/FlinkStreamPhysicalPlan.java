/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink.plan;

import com.datasqrl.FlinkExecutablePlan;
import com.datasqrl.engine.stream.StreamPhysicalPlan;
import com.datasqrl.serializer.Deserializer;
import java.io.IOException;
import java.nio.file.Path;
import lombok.Value;

@Value
public class FlinkStreamPhysicalPlan implements StreamPhysicalPlan {

  FlinkExecutablePlan executablePlan;


  public static final String FLINK_PLAN_FILENAME = "flink-plan.json";

  @Override
  public void writeTo(Path deployDir, String stageName, Deserializer serializer)
      throws IOException {
    serializer.writeJson(deployDir.resolve(FLINK_PLAN_FILENAME), executablePlan);
  }
}
