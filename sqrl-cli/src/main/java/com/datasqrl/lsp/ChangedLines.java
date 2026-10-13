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
package com.datasqrl.lsp;

import java.util.OptionalInt;

/**
 * Maps the lines of a script as it was last checked to its current lines, so that the positions of
 * the check can be used while the script is edited. The lines before and after the edited lines are
 * kept, and the edited lines are left out.
 */
final class ChangedLines {

  private final int unchangedBefore;
  private final int checkedLines;
  private final int unchangedAfter;
  private final int lineDelta;

  private ChangedLines(int unchangedBefore, int checkedLines, int unchangedAfter, int lineDelta) {
    this.unchangedBefore = unchangedBefore;
    this.checkedLines = checkedLines;
    this.unchangedAfter = unchangedAfter;
    this.lineDelta = lineDelta;
  }

  static ChangedLines between(String checked, String current) {
    var before = checked.split("\n", -1);
    var after = current.split("\n", -1);
    var prefix = 0;
    while (prefix < before.length
        && prefix < after.length
        && before[prefix].equals(after[prefix])) {
      prefix++;
    }
    var suffix = 0;
    while (suffix < before.length - prefix
        && suffix < after.length - prefix
        && before[before.length - 1 - suffix].equals(after[after.length - 1 - suffix])) {
      suffix++;
    }
    return new ChangedLines(prefix, before.length, suffix, after.length - before.length);
  }

  /** The current line of a line as it was checked, or empty if the line was edited. */
  OptionalInt line(int checkedLine) {
    if (checkedLine < unchangedBefore) {
      return OptionalInt.of(checkedLine);
    } else if (checkedLine >= checkedLines - unchangedAfter && checkedLine < checkedLines) {
      return OptionalInt.of(checkedLine + lineDelta);
    }
    return OptionalInt.empty();
  }
}
