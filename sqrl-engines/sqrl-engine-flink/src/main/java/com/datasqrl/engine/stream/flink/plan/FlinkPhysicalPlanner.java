/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink.plan;

import com.datasqrl.FlinkExecutablePlan;
import com.datasqrl.FlinkExecutablePlan.FlinkBase;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.Query;
import com.google.inject.Inject;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.functions.UserDefinedFunction;

@AllArgsConstructor(onConstructor_=@Inject)
@Slf4j
public class FlinkPhysicalPlanner {
  SqrlToFlinkExecutablePlan sqrlToFlinkExecutablePlan;

  RelBuilder relBuilder;

  public static final String ERROR_SINK_NAME = "errors_internal_sink";


  @SneakyThrows
  public FlinkStreamPhysicalPlan createStreamGraph(
      SqrlConfig config, List<? extends Query> streamQueries, TableSink errorSink, Set<URL> jars,
      Map<String, UserDefinedFunction> udfs) {
    FlinkBase flinkBase = sqrlToFlinkExecutablePlan.create(config, streamQueries, udfs, jars);
    return new FlinkStreamPhysicalPlan(new FlinkExecutablePlan(flinkBase));
  }
}
