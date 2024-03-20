/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.stream.StreamEngine;
import lombok.NonNull;
import lombok.Value;

@Value
/**
 * TODO: need to add proper error handling
 */
public class EngineSettings {

  @NonNull
  ExecutionPipeline pipeline;

  @NonNull
  StreamEngine stream;
}
