/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import static com.datasqrl.engine.EngineFeature.STANDARD_STREAM;

import java.io.IOException;
import java.util.EnumSet;

import com.datasqrl.config.EngineType;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.stream.StreamEngine;

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
