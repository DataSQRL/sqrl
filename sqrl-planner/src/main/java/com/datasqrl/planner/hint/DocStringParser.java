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
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Parses Rust-style documentation strings to extract column and argument definitions.
 *
 * <p>Supports headers like "Columns", "Column", "Arguments", "Argument" (case-insensitive),
 * optionally followed by punctuation. Under each header, items are specified as "- name:
 * description" pairs. An item that is indented more than the item above it documents a field nested
 * in that item, to any depth.
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
 *
 * <p>When every line after the first starts with an indented {@code *}, as in a {@code /** ...
 * *}{@code /} comment that starts each line with {@code " * "}, that {@code *} and the space after
 * it are removed from each line first.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DocStringParser {

  private static final Pattern SECTION_HEADER_PATTERN =
      Pattern.compile("^\\s*#*\\s*(columns?|arguments?)\\s*[:\\-]?\\s*$", Pattern.CASE_INSENSITIVE);

  private static final Pattern LIST_ITEM_PATTERN =
      Pattern.compile("^(\\s*)[-*]\\s*([\\w]+)\\s*:\\s*(.+)$");

  /**
   * The {@code *} that starts a line of a doc comment, with the space after it. It is followed by a
   * space or ends the line, so Markdown bold ({@code **}) is not one.
   */
  private static final Pattern LINE_DECORATION_PATTERN = Pattern.compile("^\\s*\\*(?: |$)");

  /**
   * A line of a doc comment that starts with the decoration. The decoration is indented, as in
   * {@code " * "}, unlike a {@code *} list item that starts the line.
   */
  private static final Pattern DECORATED_LINE_PATTERN = Pattern.compile("^\\s+\\*(?: |$)");

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

    var lines = removeLineDecoration(Arrays.asList(docString.split("\\r?\\n")));
    var currentSection = SectionType.NONE;
    boolean inDefinitionBlock = false;
    // The items above the current one that it may be nested in, the innermost on top
    Deque<ListItem> parents = new ArrayDeque<>();

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
        if (itemMatch.matches()) {
          var indentation = indentation(itemMatch.group(1));
          while (!parents.isEmpty() && parents.peek().indentation() >= indentation) {
            parents.pop();
          }
          var parent = parents.isEmpty() ? NamePath.ROOT : parents.peek().path();
          var path = parent.concat(Name.system(itemMatch.group(2).trim()));
          parents.push(new ListItem(indentation, path));
          var description = itemMatch.group(3).trim();

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

  /**
   * Removes the {@code *} that starts each line of a doc comment, if every line after the first,
   * other than blank lines, starts with an indented one. The first line has no indentation, since
   * the doc string is trimmed.
   */
  private static List<String> removeLineDecoration(List<String> lines) {
    var decorated =
        lines.size() > 1
            && lines.stream()
                .skip(1)
                .filter(line -> !line.isBlank())
                .allMatch(line -> DECORATED_LINE_PATTERN.matcher(line).lookingAt());
    if (!decorated) {
      return lines;
    }
    return lines.stream()
        .map(line -> LINE_DECORATION_PATTERN.matcher(line).replaceFirst(""))
        .toList();
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
