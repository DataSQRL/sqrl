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

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ExplainConfigImpl implements PackageJson.ExplainConfig {

  private final SqrlConfig sqrlConfig;

  @Override
  public boolean isText() {
    return sqrlConfig.asBool("text").get();
  }

  @Override
  public boolean isSql() {
    return sqrlConfig.asBool("sql").get();
  }

  @Override
  public boolean isLogical() {
    return sqrlConfig.asBool("logical").get();
  }

  @Override
  public boolean isPhysical() {
    return sqrlConfig.asBool("physical").get();
  }

  @Override
  public boolean isSorted() {
    return sqrlConfig.asBool("sorted").get();
  }

  @Override
  public boolean isVisual() {
    return sqrlConfig.asBool("visual").get();
  }
}
