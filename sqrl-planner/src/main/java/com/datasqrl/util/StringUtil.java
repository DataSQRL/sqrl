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
package com.datasqrl.util;

import com.google.common.base.Preconditions;

public class StringUtil {

  public static String removeFromEnd(String base, String remove) {
    Preconditions.checkArgument(
        base.length() >= remove.length(), "Invalid removal [%s] for [%s]", remove, base);
    Preconditions.checkArgument(base.endsWith(remove), "[%s] does not end in [%s]", base, remove);

    return base.substring(0, base.length() - remove.length());
  }

  public static String replaceSubstring(String original, int start, int end, String replacement) {
    if (start < 0 || end > original.length() || start > end) {
      throw new IllegalArgumentException("Invalid start or end position");
    }
    var before = original.substring(0, start);
    var after = original.substring(end);
    return before + replacement + after;
  }
}
