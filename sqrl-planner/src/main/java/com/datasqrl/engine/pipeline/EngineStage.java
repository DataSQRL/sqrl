/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.pipeline;

import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.ExecutionEngine;
import lombok.Value;

@Value
public class EngineStage implements ExecutionStage {

  String name;
  ExecutionEngine engine;


  @Override
  public boolean supportsFeature(EngineFeature capability) {
    return engine.supports(capability);
  }

//  @Override
//  public boolean supportsFunction(FunctionDefinition function) {
//    return engine.supports(function);
//  }


}
