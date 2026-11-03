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
package com.datasqrl.planner.tags;

import com.datasqrl.error.ErrorCollector;

/**
 * How the tags of an analyzed table or function are computed and validated.
 *
 * @param definitions the tag definitions; tags are not analyzed when disabled
 * @param tableName the table name used in error messages
 * @param validateDeclared whether the table must declare the tags it inherits (public tables and
 *     functions, but not hidden tables or relationships)
 * @param errors the error collector for tag errors, null to use the analyzer's error collector
 */
public record TagContext(
    TagDefinitions definitions, String tableName, boolean validateDeclared, ErrorCollector errors) {

  public static final TagContext DISABLED =
      new TagContext(TagDefinitions.DISABLED, null, false, null);

  public boolean isEnabled() {
    return definitions.isEnabled();
  }
}
