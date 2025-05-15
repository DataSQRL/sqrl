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

import com.datasqrl.util.StringUtil;
import com.google.common.base.Preconditions;
import java.util.Optional;
import java.util.regex.Pattern;

public class TablePattern {

  private final String pattern;

  private TablePattern(String pattern) {
    Preconditions.checkArgument(isValid(pattern), "Invalid table pattern: %s", pattern);
    this.pattern = pattern;
  }

  public Pattern get(boolean withBoundary) {
    var updatedPattern = pattern;
    if (withBoundary) {
      updatedPattern = addRegexBoundary(pattern);
    }
    return Pattern.compile(updatedPattern);
  }

  public String substitute(String name, Optional<String> prefix, Optional<String> suffix) {
    var nameOffsets = getNameMatchOffsets(pattern);
    var updatedPattern =
        StringUtil.replaceSubstring(pattern, nameOffsets[0], nameOffsets[1], Pattern.quote(name));
    if (prefix.isPresent()) {
      updatedPattern = Pattern.quote(prefix.get()) + updatedPattern;
    }
    if (suffix.isPresent()) {
      updatedPattern = updatedPattern + Pattern.quote(suffix.get());
    }
    return updatedPattern;
  }

  public static TablePattern of(Optional<String> pattern, String defaultPattern) {
    return new TablePattern(pattern.orElse(defaultPattern));
  }

  public static boolean isValid(String pattern) {
    try {
      Pattern.compile(pattern);
      return getNameMatchOffsets(pattern) != null;
    } catch (Exception e) {
      return false;
    }
  }

  private static String addRegexBoundary(String pattern) {
    return "^" + pattern + "$";
  }

  public static int[] getNameMatchOffsets(String pattern) {
    var startPos = -1;
    var endPos = -1;
    for (var i = 0; i < pattern.length() - 1; i++) {
      if (pattern.charAt(i) == '(' && pattern.charAt(i + 1) != '?') {
        startPos = i;
        break;
      }
    }

    if (startPos != -1) {
      for (var i = startPos + 1; i < pattern.length(); i++) {
        if (pattern.charAt(i) == ')') {
          endPos = i;
          break;
        }
      }
    }

    if (startPos < 0 || endPos < 0) {
      return null;
    }
    return new int[] {startPos, endPos};
  }
}
