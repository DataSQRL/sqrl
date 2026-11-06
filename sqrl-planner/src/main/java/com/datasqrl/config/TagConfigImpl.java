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

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;

/** A tag definition under {@code compiler.tags.<name>}. */
@AllArgsConstructor
public class TagConfigImpl implements PackageJson.TagConfig {

  private final SqrlConfig sqrlConfig;

  @Override
  public List<String> getValues() {
    return sqrlConfig.asList("values", String.class).get();
  }

  @Override
  public Map<String, String> getRequiredColumns() {
    return sqrlConfig.asMap("required-columns", String.class).get();
  }
}
