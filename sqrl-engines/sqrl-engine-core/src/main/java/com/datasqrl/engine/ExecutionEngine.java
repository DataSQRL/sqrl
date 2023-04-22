/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine;

import com.datasqrl.config.SerializedSqrlConfig;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.PhysicalDAGPlan;
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
   * Returns the {@link DataSystemConnector} for this engine so it can
   * be used as a sink by a previous stage in the pipeline.
   * @return
   */
  SqrlConfig getConnectorConfig();

  ExecutionResult execute(EnginePhysicalPlan plan);

  EnginePhysicalPlan plan(PhysicalDAGPlan.StagePlan plan, List<PhysicalDAGPlan.StageSink> inputs,
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
    public SqrlConfig getConnectorConfig() {
      throw new UnsupportedOperationException("Not a sink");
    }
  }

}
