/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink.plan;

import com.datasqrl.FlinkExecutablePlan;
import com.datasqrl.FlinkExecutablePlan.FlinkBase;
import com.datasqrl.engine.stream.flink.sql.SqrlToFlinkExecutablePlan;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.Query;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.functions.UserDefinedFunction;

@AllArgsConstructor
@Slf4j
public class FlinkPhysicalPlanner {

  RelBuilder relBuilder;

  public static final String ERROR_SINK_NAME = "errors_internal_sink";


  @SneakyThrows
  public FlinkStreamPhysicalPlan createStreamGraph(
      List<? extends Query> streamQueries, TableSink errorSink, Set<URL> jars,
      Map<String, UserDefinedFunction> udfs) {
    SqrlToFlinkExecutablePlan sqrlToFlinkExecutablePlan = new SqrlToFlinkExecutablePlan(errorSink);
    FlinkBase flinkBase = sqrlToFlinkExecutablePlan.create(streamQueries, udfs, jars);
    return new FlinkStreamPhysicalPlan(new FlinkExecutablePlan(flinkBase));
  }
}
