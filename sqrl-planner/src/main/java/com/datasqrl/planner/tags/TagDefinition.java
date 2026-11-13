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

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;

/**
 * A tag defined under {@code compiler.tags}, with its name and values normalized to lower case.
 *
 * <p>The values are ordered from lowest to highest, so the index of a value is its level. A boolean
 * tag has the single value {@code true} and is written without a value.
 *
 * @param name the tag name
 * @param values the allowed values, ordered from lowest to highest
 * @param isBoolean whether the tag is a boolean tag
 * @param requiredColumns the columns every table carrying the tag must have
 */
public record TagDefinition(
    String name, List<String> values, boolean isBoolean, List<RequiredColumn> requiredColumns) {

  public static final String TRUE = "true";

  /** Creates a boolean tag if there are no values, and an ordered tag otherwise. */
  public static TagDefinition of(
      String name, List<String> values, List<RequiredColumn> requiredColumns) {

    return values.isEmpty()
        ? new TagDefinition(name, List.of(TRUE), true, requiredColumns)
        : new TagDefinition(name, List.copyOf(values), false, requiredColumns);
  }

  /**
   * A column that every table carrying the tag must have.
   *
   * @param name the column name
   * @param typeName the type as configured
   * @param type the parsed type
   */
  public record RequiredColumn(String name, String typeName, RelDataType type) {}
}
