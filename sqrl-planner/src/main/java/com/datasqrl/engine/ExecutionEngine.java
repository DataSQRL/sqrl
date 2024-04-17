/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.config.TableConfig;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;

import com.datasqrl.function.DowncastFunction;
import com.datasqrl.functions.json.JsonDowncastFunction;
import com.datasqrl.functions.vector.VectorDowncastFunction;
import com.datasqrl.json.FlinkJsonType;
import com.datasqrl.json.JsonToString;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.vector.FlinkVectorType;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

/**
 * Describes a physical execution engine and it's capabilities.
 */
public interface ExecutionEngine extends IExecutionEngine{

  boolean supports(EngineFeature capability);

  boolean supports(FunctionDefinition function);

  /**
   * Returns the {@link TableConfig} for this engine so it can
   * be used as a sink by a previous stage in the pipeline.
   * @return
   */
  TableConfig getSinkConfig(String sinkName);

//  CompletableFuture<ExecutionResult> execute(EnginePhysicalPlan plan, ErrorCollector errors);

  EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs,
      ExecutionPipeline pipeline, SqrlFramework relBuilder,
      ErrorCollector errorCollector);

  @AllArgsConstructor
  @Getter
  abstract class Base implements ExecutionEngine {

    protected final @NonNull String name;
    protected final @NonNull Type type;
    protected final @NonNull EnumSet<EngineFeature> capabilities;

    @Override
    public boolean supports(EngineFeature capability) {
      return capabilities.contains(capability);
    }

    @Override
    public boolean supports(FunctionDefinition function) {
      return false;
    }

    @Override
    public TableConfig getSinkConfig(String sinkName) {
      throw new UnsupportedOperationException("Not a sink");
    }
  }

  default Optional<DowncastFunction> getDowncastFunction(RelDataType type) {
    // Convert sqrl native raw types to strings
    if (type instanceof RawRelDataType) {
      if ((((RawRelDataType)type).getRawType().getDefaultConversion() == FlinkJsonType.class)) {
        return Optional.of(new JsonDowncastFunction());
      } else if ((((RawRelDataType)type).getRawType().getDefaultConversion() == FlinkVectorType.class)) {
        return Optional.of(new VectorDowncastFunction());
      }
    }

    return Optional.empty(); //assume everything is supported by default
  }
}
