/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink.plan;

import com.datasqrl.FlinkExecutablePlan;
import com.datasqrl.engine.stream.StreamPhysicalPlan;
import java.net.URL;
import java.util.Set;
import lombok.Value;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;

@Value
public class FlinkStreamPhysicalPlan implements StreamPhysicalPlan {

  FlinkExecutablePlan executablePlan;

  //TODO: Make the jars work in executable plan (if local)
  Set<URL> jars;

}
