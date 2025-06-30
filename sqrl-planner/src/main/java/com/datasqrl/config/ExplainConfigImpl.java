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
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Getter
@Builder
@AllArgsConstructor
public class ExplainConfigImpl implements PackageJson.ExplainConfig {

  /** Whether to produce a visual explanation */
  @Default boolean visual = true;

  /** Whether to produce a textual explanation */
  @Default boolean text = true;

  /** Whether to print the SQL for the tables and queries as planned */
  @Default boolean sql = false;

  /**
   * Whether to print the logical plan for the tables and queries as a relational tree with hints
   */
  @Default boolean logical = true;

  /** Whether to print the physical plan for the tables and queries */
  @Default boolean physical = false;

  /**
   * This setting is primarily used for testing to ensure that the output of explain is
   * deterministic
   */
  @Default boolean sorted = true; // TODO: set to false and overwrite in test case injector

  public ExplainConfigImpl(SqrlConfig sqrlConfig) {
    this(
        sqrlConfig.asBool("visual").getOptional().orElse(true),
        sqrlConfig.asBool("text").getOptional().orElse(true),
        sqrlConfig.asBool("sql").getOptional().orElse(false),
        sqrlConfig.asBool("logical").getOptional().orElse(true),
        sqrlConfig.asBool("physical").getOptional().orElse(false),
        sqrlConfig.asBool("sorted").getOptional().orElse(true));
  }
}
