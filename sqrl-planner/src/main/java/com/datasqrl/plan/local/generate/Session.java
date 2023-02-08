/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.local.generate;

import static java.util.Objects.requireNonNull;

import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.calcite.SqrlRelBuilder;
import com.datasqrl.plan.calcite.TypeFactory;
import com.datasqrl.plan.calcite.hints.SqrlHintStrategyTable;
import com.datasqrl.plan.calcite.rules.SqrlRelMetadataProvider;
import com.datasqrl.plan.calcite.rules.SqrlRelMetadataQuery;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.RelBuilder;

@AllArgsConstructor
@Getter
public class Session {

  @Setter
  ErrorCollector errors;
  ExecutionPipeline pipeline;
  DebuggerConfig debugger;

  RelOptCluster cluster;

  RelOptPlanner relPlanner;
  SqrlCalciteSchema schema;

  public static Session createSession(ErrorCollector errors,
      ExecutionPipeline pipeline, DebuggerConfig debuggerConfig, SqrlCalciteSchema schema) {

    RelOptPlanner planner = new VolcanoPlanner(null, Contexts.empty());

    RelOptCluster cluster = RelOptCluster.create(
        requireNonNull(planner, "planner"),
        new RexBuilder(TypeFactory.getTypeFactory()));
    cluster.setMetadataProvider(new SqrlRelMetadataProvider());
    cluster.setMetadataQuerySupplier(SqrlRelMetadataQuery::new);
    cluster.setHintStrategies(SqrlHintStrategyTable.getHintStrategyTable());

    return new Session(errors, pipeline, debuggerConfig, cluster, planner, schema);
  }

  public RelBuilder createRelBuilder() {
    return SqrlRelBuilder.create(cluster, schema);
  }
}