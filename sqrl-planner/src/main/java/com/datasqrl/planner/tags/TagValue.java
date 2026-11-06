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

/**
 * A tag with one of its values.
 *
 * @param definition the tag definition
 * @param level the index of the value in the definition's values, higher is a higher value
 */
public record TagValue(TagDefinition definition, int level) {

  public String name() {
    return definition.name();
  }

  public String value() {
    return definition.values().get(level);
  }

  public boolean isLowerThan(TagValue other) {
    return level < other.level;
  }

  /** The tag as written in a doc string, e.g. {@code @pii=raw} or {@code @gdpr}. */
  @Override
  public String toString() {
    return definition.isBoolean() ? "@" + name() : "@" + name() + "=" + value();
  }
}
