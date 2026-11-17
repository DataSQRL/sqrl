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
package com.datasqrl.planner.hint;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.planner.util.Documented.Documentation;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Parses Rust-style documentation strings to extract column and argument definitions.
 *
 * <p>Supports headers like "Columns", "Column", "Arguments", "Argument" (case-insensitive),
 * optionally followed by punctuation. Under each header, items are specified as "- name:
 * description" pairs, also with {@code *} as the bullet. An item that is indented more than the
 * item above it documents a field nested in that item, to any depth. The items of a list whose
 * first item has no bullet need none, like "name: description" in a doc comment that starts each
 * line with {@code *}, after the comment parser removed that {@code *}.
 *
 * <p>Example:
 *
 * <pre>{@code
 * This is the main description.
 *
 * # Columns:
 * - id: The unique identifier
 * - address: The postal address
 *   - street: The street and house number
 *   - geo: The location
 *     - lat: The latitude
 *
 * # Arguments
 * - limit: Maximum number of results
 * }</pre>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DocStringParser {

  private static final Pattern SECTION_HEADER_PATTERN =
      Pattern.compile("^\\s*#*\\s*(columns?|arguments?)\\s*[:\\-]?\\s*$", Pattern.CASE_INSENSITIVE);

  /** A list item: its indentation, its optional bullet, its name and its description. */
  private static final Pattern LIST_ITEM_PATTERN =
      Pattern.compile("^(\\s*)([-*]\\s*)?(\\w+)\\s*:\\s*(.+)$");

  private static final int TAB_WIDTH = 4;

  private enum SectionType {
    NONE,
    COLUMN,
    ARGUMENT
  }

  /** An item of a list, with its indentation. */
  private record ListItem(int indentation, NamePath path) {}

  /**
   * Parses a docstring to extract column and argument documentation.
   *
   * @param docString the raw documentation string to parse
   * @return a Documentation record containing the remaining docString and extracted column/argument
   *     docs
   */
  public static Documentation parse(String docString) {
    if (docString == null || docString.isBlank()) {
      return new Documentation(null, Map.of(), Map.of());
    }

    var columnDocs = new LinkedHashMap<NamePath, String>();
    var argumentDocs = new LinkedHashMap<NamePath, String>();
    var remainingDoc = new StringBuilder();

    var lines = docString.split("\\r?\\n");
    var currentSection = SectionType.NONE;
    boolean inDefinitionBlock = false;
    // The items above the current one that it may be nested in, the innermost on top
    Deque<ListItem> parents = new ArrayDeque<>();
    // Whether the items of the current list start with a bullet
    boolean bulletedList = true;

    for (var line : lines) {
      var detectedSection = detectSectionHeader(line);

      if (detectedSection != SectionType.NONE) {
        currentSection = detectedSection;
        inDefinitionBlock = true;
        parents.clear();
        continue;
      }

      if (inDefinitionBlock) {
        var itemMatch = LIST_ITEM_PATTERN.matcher(line);
        var matches = itemMatch.matches();
        var hasBullet = matches && itemMatch.group(2) != null;
        if (matches && parents.isEmpty()) {
          // The first item of a list decides whether its items start with a bullet
          bulletedList = hasBullet;
        }
        if (matches && (hasBullet || !bulletedList)) {
          var indentation = indentation(itemMatch.group(1));
          while (!parents.isEmpty() && parents.peek().indentation() >= indentation) {
            parents.pop();
          }
          var parent = parents.isEmpty() ? NamePath.ROOT : parents.peek().path();
          var path = parent.concat(Name.system(itemMatch.group(3).trim()));
          parents.push(new ListItem(indentation, path));
          var description = itemMatch.group(4).trim();

          if (currentSection == SectionType.COLUMN) {
            columnDocs.put(path, description);

          } else if (currentSection == SectionType.ARGUMENT) {
            argumentDocs.put(path, description);
          }
        } else if (line.isBlank()) {
          // Blank line after list items ends the definition block
          // But only if we've already parsed some items
          if ((currentSection == SectionType.COLUMN && !columnDocs.isEmpty())
              || (currentSection == SectionType.ARGUMENT && !argumentDocs.isEmpty())) {
            inDefinitionBlock = false;
            currentSection = SectionType.NONE;
          }
        } else if (!line.trim().startsWith("-") && !line.trim().startsWith("*")) {
          // Non-list line that isn't blank - this line belongs to the doc
          inDefinitionBlock = false;
          currentSection = SectionType.NONE;
          appendToRemainingDoc(remainingDoc, line);
        }
      } else {
        appendToRemainingDoc(remainingDoc, line);
      }
    }

    var finalDocString = remainingDoc.toString().trim();

    return new Documentation(
        finalDocString.isEmpty() ? null : finalDocString, columnDocs, argumentDocs);
  }

  /** Returns the width of the indentation, with a tab as {@value #TAB_WIDTH} spaces. */
  private static int indentation(String whitespace) {
    return whitespace.chars().map(c -> c == '\t' ? TAB_WIDTH : 1).sum();
  }

  private static SectionType detectSectionHeader(String line) {
    var matcher = SECTION_HEADER_PATTERN.matcher(line);

    if (matcher.matches()) {
      var headerType = matcher.group(1).toLowerCase();

      if (headerType.startsWith("column")) {
        return SectionType.COLUMN;
      }

      if (headerType.startsWith("argument")) {
        return SectionType.ARGUMENT;
      }
    }

    return SectionType.NONE;
  }

  private static void appendToRemainingDoc(StringBuilder sb, String line) {
    if (!sb.isEmpty()) {
      sb.append("\n");
    }
    sb.append(line);
  }
}
