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

import com.datasqrl.planner.analyzer.cost.CostModel;
import com.datasqrl.planner.analyzer.cost.SimpleCostAnalysisModel;
import com.datasqrl.planner.analyzer.cost.SimpleCostAnalysisModel.Type;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class CompilerConfigImpl implements PackageJson.CompilerConfig {

  SqrlConfig sqrlConfig;

  @Override
  public boolean isExtendedScalarTypes() {
    return sqrlConfig.asBool("extendedScalarTypes").getOptional().orElse(true);
  }

  @Override
  public String getLogger() {
    return sqrlConfig.hasKey("logger") ? sqrlConfig.asString("logger").get() : "print";
  }

  @Override
  public boolean compilePlan() {
    return sqrlConfig.asBool("compilePlan").withDefault(true).get();
  }

  @Override
  public CostModel getCostModel() {
    return new SimpleCostAnalysisModel(
        sqrlConfig
            .asString("cost-model")
            .validate(
                str -> SimpleCostAnalysisModel.Type.fromString(str).isPresent(),
                "Not a valid cost model, must be one of " + SimpleCostAnalysisModel.Type.values())
            .getOptional()
            .flatMap(SimpleCostAnalysisModel.Type::fromString)
            .orElse(Type.DEFAULT));
  }

  @Override
  public ExplainConfigImpl getExplain() {
    return new ExplainConfigImpl(sqrlConfig.getSubConfig("explain"));
  }

  @Override
  public CompilerApiConfigImpl getApiConfig() {
    return CompilerApiConfigImpl.from(sqrlConfig.getSubConfig("api"));
  }
}
