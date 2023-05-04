/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine;

import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
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
   * Returns the {@link com.datasqrl.io.tables.TableConfig} for this engine so it can
   * be used as a sink by a previous stage in the pipeline.
   * @return
   */
  TableConfig getSinkConfig(String sinkName);

  CompletableFuture<ExecutionResult> execute(EnginePhysicalPlan plan, ErrorCollector errors);

  EnginePhysicalPlan plan(PhysicalDAGPlan.StagePlan plan, List<PhysicalDAGPlan.StageSink> inputs,
      ExecutionPipeline pipeline, RelBuilder relBuilder, TableSink errorSink);


  default void generateAssets(Path buildDir) {
  }

  @AllArgsConstructor
  @Getter
  @Slf4j
  public static abstract class Base implements ExecutionEngine {

    protected final @NonNull String name;
    protected final @NonNull Type type;
    protected final @NonNull EnumSet<EngineCapability> capabilities;

    @Override
    public boolean supports(EngineCapability capability) {
      return capabilities.contains(capability);
    }

    @Override
    public TableConfig getSinkConfig(String sinkName) {
      throw new UnsupportedOperationException("Not a sink");
    }

    public void generateAssets(Path buildDir) {
      log.debug("Not generating assets for: " + getName());
    }
  }

}
