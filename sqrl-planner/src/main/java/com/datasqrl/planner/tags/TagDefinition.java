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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.OptionalInt;
import org.apache.calcite.rel.type.RelDataType;

/**
 * The definition of a tag from {@code compiler.tags}. Names and values are normalized to lower
 * case.
 *
 * <p>A tag without values is a boolean tag whose only value is {@code true}, represented by the
 * offset {@link #BOOLEAN_OFFSET}. For ordered tags, a value is represented by its offset into
 * {@link #values()}, so higher offsets are higher values.
 *
 * @param name normalized tag name
 * @param values allowed values ordered from lowest to highest, empty for boolean tags
 * @param requiredColumns columns every table carrying this tag must have
 */
public record TagDefinition(
    String name, List<String> values, List<RequiredColumn> requiredColumns) {

  public static final String TRUE = "true";
  public static final int BOOLEAN_OFFSET = 1;

  public boolean isBoolean() {
    return values.isEmpty();
  }

  /** Returns the offset for the given normalized value, or empty if the value is not valid. */
  public OptionalInt offsetOf(String value) {
    if (isBoolean()) {
      return TRUE.equals(value) ? OptionalInt.of(BOOLEAN_OFFSET) : OptionalInt.empty();
    }
    var offset = values.indexOf(value);
    return offset < 0 ? OptionalInt.empty() : OptionalInt.of(offset);
  }

  /** Returns the value name for the given offset. */
  public String valueAt(int offset) {
    if (isBoolean()) {
      Preconditions.checkArgument(offset == BOOLEAN_OFFSET, "Invalid boolean tag offset");
      return TRUE;
    }
    return values.get(offset);
  }

  /**
   * A column that every table carrying the tag must have.
   *
   * @param name the column name
   * @param typeDefinition the type as configured
   * @param type the parsed type
   */
  public record RequiredColumn(String name, String typeDefinition, RelDataType type) {}
}
