/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine;

import com.datasqrl.io.DataSystemConnectorConfig;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.OptimizedDAG;
import java.util.EnumSet;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.tools.RelBuilder;

/**
 * Describes a physical execution engine and it's capabilities.
 */
public interface ExecutionEngine {

  public enum Type {
    STREAM, DATABASE, SERVER;

    public boolean isWrite() {
      return this == STREAM;
    }

    public boolean isRead() {
      return this == DATABASE || this == SERVER;
    }
  }

  boolean supports(EngineCapability capability);

  Type getType();

  String getName();

  /**
   * Returns the {@link DataSystemConnectorConfig} for this engine so it can
   * be used as a sink by a previous stage in the pipeline.
   * @return
   */
  DataSystemConnectorConfig getDataSystemConnectorConfig();

  ExecutionResult execute(EnginePhysicalPlan plan);

  EnginePhysicalPlan plan(OptimizedDAG.StagePlan plan, List<OptimizedDAG.StageSink> inputs,
      RelBuilder relBuilder, TableSink errorSink);

  @AllArgsConstructor
  @Getter
  public static abstract class Base implements ExecutionEngine {

    protected final @NonNull String name;
    protected final @NonNull Type type;
    protected final @NonNull EnumSet<EngineCapability> capabilities;

    @Override
    public boolean supports(EngineCapability capability) {
      return capabilities.contains(capability);
    }

    @Override
    public DataSystemConnectorConfig getDataSystemConnectorConfig() {
      throw new UnsupportedOperationException("Not a sink");
    }
  }

}
