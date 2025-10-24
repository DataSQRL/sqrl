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
    return sqrlConfig.asBool("extended-scalar-types").get();
  }

  @Override
  public String getLogger() {
    return sqrlConfig.asString("logger").get();
  }

  @Override
  public boolean compileFlinkPlan() {
    return sqrlConfig.asBool("compile-flink-plan").get();
  }

  @Override
  public boolean addIcebergSerializationConfig() {
    return sqrlConfig.asBool("add-iceberg-serialization-config").get();
  }

  @Override
  public CostModel getCostModel() {
    var costModel = sqrlConfig.as("cost-model", Type.class).get();

    return new SimpleCostAnalysisModel(costModel);
  }

  @Override
  public ExplainConfigImpl getExplain() {
    return new ExplainConfigImpl(sqrlConfig.getSubConfig("explain"));
  }

  @Override
  public CompilerApiConfigImpl getApiConfig() {
    return new CompilerApiConfigImpl(sqrlConfig.getSubConfig("api"));
  }
}
