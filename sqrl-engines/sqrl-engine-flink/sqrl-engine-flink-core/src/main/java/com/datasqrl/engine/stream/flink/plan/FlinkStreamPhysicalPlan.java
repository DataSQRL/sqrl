/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink.plan;

import com.datasqrl.FlinkExecutablePlan;
import com.datasqrl.engine.stream.StreamPhysicalPlan;
import lombok.Value;

@Value
public class FlinkStreamPhysicalPlan implements StreamPhysicalPlan {

  FlinkExecutablePlan executablePlan;

}
