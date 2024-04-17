/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink.plan;

import com.datasqrl.engine.stream.StreamPhysicalPlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StreamStagePlan;
import com.datasqrl.serializer.Deserializer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import lombok.Value;

@Value
public class FlinkStreamPhysicalPlan implements StreamPhysicalPlan {
  StreamStagePlan plan;
  List<String> flinkSql;

  public static final String FLINK_PLAN_FILENAME = "flink-plan.json";
}
