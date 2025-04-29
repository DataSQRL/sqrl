/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import static com.datasqrl.engine.EngineFeature.STANDARD_STREAM;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.EngineType;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.stream.StreamEngine;
import com.datasqrl.engine.stream.flink.plan.FlinkStreamPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.StreamStagePlan;
import com.google.common.base.Preconditions;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractFlinkStreamEngine extends ExecutionEngine.Base implements
    StreamEngine {

  public static final EnumSet<EngineFeature> FLINK_CAPABILITIES = STANDARD_STREAM;

  @Getter
  private final EngineConfig config;

  public AbstractFlinkStreamEngine(EngineConfig config) {
    super(FlinkEngineFactory.ENGINE_NAME, EngineType.STREAMS, FLINK_CAPABILITIES);
    this.config = config;
  }

  @Override
  public void close() throws IOException {
  }

}
