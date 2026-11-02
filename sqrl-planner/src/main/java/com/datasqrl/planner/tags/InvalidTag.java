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
 * A tag in a doc string that does not resolve to a defined tag and value.
 *
 * @param text the tag as written (normalized), e.g. {@code @pii=unknown}
 * @param name the normalized tag name
 * @param reason why the tag is invalid
 */
public record InvalidTag(String text, String name, Reason reason) {

  public enum Reason {
    UNDEFINED_TAG,
    INVALID_VALUE,
    MISSING_VALUE,
    NON_TRUE_BOOLEAN
  }

  /** A message describing the problem, used in compile errors. */
  public String message(TagDefinitions definitions) {
    return switch (reason) {
      case UNDEFINED_TAG -> "Tag [%s] is not defined in compiler.tags".formatted(text);
      case INVALID_VALUE ->
          "Invalid value in tag [%s]: allowed values are %s"
              .formatted(text, definitions.getOrThrow(name).values());
      case MISSING_VALUE ->
          "Tag [%s] requires a value: allowed values are %s"
              .formatted(text, definitions.getOrThrow(name).values());
      case NON_TRUE_BOOLEAN ->
          "Tag [%s] is a boolean tag and only accepts the value [true]".formatted(text);
    };
  }
}
