/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan;

import com.datasqrl.config.EngineType;
import com.datasqrl.engine.stream.flink.sql.rules.ToStubAggRule.ToStubAggRuleConfig;
import com.datasqrl.plan.rules.DAGFunctionExpansionRule;
import com.datasqrl.plan.rules.DAGTableExpansionRule.Read;
import com.datasqrl.plan.rules.DAGTableExpansionRule.Write;
import com.datasqrl.plan.rules.SQRLPrograms;
import com.datasqrl.plan.rules.SqrlRelMetadataProvider;
import lombok.Value;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * An {@link OptimizationStage}
 */
@Value
public class OptimizationStage {

  private static final List<OptimizationStage> ALL_STAGES = new ArrayList<>();

  private final int index;
  private final String name;
  private final Program program;
  private final Optional<RelTrait> trait;

  public OptimizationStage(String name, Program program, Optional<RelTrait> trait) {
    this.name = name;
    this.program = program;
    this.trait = trait;
    synchronized (ALL_STAGES) {
      this.index = ALL_STAGES.size();
      ALL_STAGES.add(index, this);
    }
  }

  public static List<Program> getAllPrograms() {
    return ALL_STAGES.stream().map(OptimizationStage::getProgram).collect(Collectors.toList());
  }

    /*
    ====== DEFINITION OF ACTUAL STAGES
     */

  public static final OptimizationStage PUSH_FILTER_INTO_JOIN = new OptimizationStage(
      "PushFilterIntoJoin",
      Programs.hep(List.of(
          CoreRules.FILTER_INTO_JOIN
      ), false, DefaultRelMetadataProvider.INSTANCE),
      Optional.empty());

  public static final OptimizationStage DATABASE_DAG_STITCHING = new OptimizationStage(
      "DatabaseDAGExpansion",
      Programs.hep(List.of(new Read(EngineType.DATABASE), new DAGFunctionExpansionRule(EngineType.DATABASE)),
          false, SqrlRelMetadataProvider.INSTANCE), Optional.empty());


  public static final OptimizationStage SERVER_DAG_STITCHING = new OptimizationStage(
      "ServerDAGExpansion",
      Programs.hep(List.of(new Read(EngineType.SERVER), new DAGFunctionExpansionRule(EngineType.SERVER)),
          false, SqrlRelMetadataProvider.INSTANCE), Optional.empty());

  public static final OptimizationStage STREAM_DAG_STITCHING = new OptimizationStage(
      "StreamDAGExpansion",
      Programs.hep(List.of(new Write()), false,
          SqrlRelMetadataProvider.INSTANCE),
      Optional.empty());


  public static final OptimizationStage VOLCANO = new OptimizationStage("Volcano",
      SQRLPrograms.ENUMERABLE_VOLCANO, Optional.of(EnumerableConvention.INSTANCE)
  );

  public static final OptimizationStage READ_QUERY_OPTIMIZATION = new OptimizationStage(
      "ReadQueryOptimization",
      Programs.sequence(
          Programs.hep(List.of(ToStubAggRuleConfig.DEFAULT.toRule()),
              true, SqrlRelMetadataProvider.INSTANCE),
          SQRLPrograms.ENUMERABLE_VOLCANO), Optional.of(EnumerableConvention.INSTANCE)
  );

  public static final OptimizationStage PUSH_DOWN_FILTERS = new OptimizationStage("PushDownFilters",
      Programs.hep(List.of(
          CoreRules.FILTER_INTO_JOIN,
          CoreRules.FILTER_MERGE,
          CoreRules.FILTER_AGGREGATE_TRANSPOSE,
          CoreRules.FILTER_PROJECT_TRANSPOSE,
          CoreRules.FILTER_TABLE_FUNCTION_TRANSPOSE,
          CoreRules.FILTER_CORRELATE,
          CoreRules.FILTER_SET_OP_TRANSPOSE
      ), false, SqrlRelMetadataProvider.INSTANCE),
      Optional.empty());

}
