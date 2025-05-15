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
public class OutputConfigImpl implements PackageJson.OutputConfig {

  /** Whether to append a unique identifier to the generate tables in the engines */
  @Default boolean addUid = true;

  /** This suffix it appended to all table names (before the uid) */
  @Default String tableSuffix = "";

  public static OutputConfigImpl from(SqrlConfig config) {
    var builder = builder();
    config.asBool("add-uid").getOptional().ifPresent(builder::addUid);
    config.asString("table-suffix").getOptional().ifPresent(builder::tableSuffix);
    return builder.build();
  }
}
