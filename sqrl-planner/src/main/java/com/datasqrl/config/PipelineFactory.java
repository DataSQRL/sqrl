/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.config;

import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.SimplePipeline;
import com.datasqrl.error.ErrorCollector;
import com.google.inject.Injector;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class PipelineFactory {

  private final Injector injector;

  public ExecutionPipeline createPipeline() {
    var engineHolder = injector.getInstance(ExecutionEnginesHolder.class);
    var errorCollector = injector.getInstance(ErrorCollector.class);

    return SimplePipeline.of(engineHolder.getEngines(), errorCollector);
  }
}
