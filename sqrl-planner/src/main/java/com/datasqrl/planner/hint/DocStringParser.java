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
import com.datasqrl.planner.util.Documented.Documentation;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses Rust-style documentation strings to extract column and argument definitions.
 *
 * <p>Supports headers like "Columns", "Column", "Arguments", "Argument" (case-insensitive),
 * optionally followed by punctuation. Under each header, items are specified as "- name:
 * description" pairs.
 *
 * <p>Example:
 *
 * <pre>{@code
 * This is the main description.
 *
 * # Columns:
 * - id: The unique identifier
 * - name: The entity name
 *
 * # Arguments
 * - limit: Maximum number of results
 * }</pre>
 */
public final class DocStringParser {

  private static final Pattern SECTION_HEADER_PATTERN =
      Pattern.compile("^\\s*#*\\s*(columns?|arguments?)\\s*[:\\-]?\\s*$", Pattern.CASE_INSENSITIVE);

  private static final Pattern LIST_ITEM_PATTERN =
      Pattern.compile("^\\s*[-*]\\s*([\\w]+)\\s*:\\s*(.+)$");

  private DocStringParser() {}

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

    Map<Name, String> columnDocs = new LinkedHashMap<>();
    Map<Name, String> argumentDocs = new LinkedHashMap<>();
    StringBuilder remainingDoc = new StringBuilder();

    String[] lines = docString.split("\\r?\\n");
    SectionType currentSection = SectionType.NONE;
    boolean inDefinitionBlock = false;

    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      SectionType detectedSection = detectSectionHeader(line);

      if (detectedSection != SectionType.NONE) {
        currentSection = detectedSection;
        inDefinitionBlock = true;
        continue;
      }

      if (inDefinitionBlock) {
        var itemMatch = LIST_ITEM_PATTERN.matcher(line);
        if (itemMatch.matches()) {
          var name = Name.system(itemMatch.group(1).trim());
          var description = itemMatch.group(2).trim();
          if (currentSection == SectionType.COLUMN) {
            columnDocs.put(name, description);
          } else if (currentSection == SectionType.ARGUMENT) {
            argumentDocs.put(name, description);
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

  private static SectionType detectSectionHeader(String line) {
    Matcher matcher = SECTION_HEADER_PATTERN.matcher(line);
    if (matcher.matches()) {
      String headerType = matcher.group(1).toLowerCase();
      if (headerType.startsWith("column")) {
        return SectionType.COLUMN;
      } else if (headerType.startsWith("argument")) {
        return SectionType.ARGUMENT;
      }
    }
    return SectionType.NONE;
  }

  private static void appendToRemainingDoc(StringBuilder sb, String line) {
    if (sb.length() > 0) {
      sb.append("\n");
    }
    sb.append(line);
  }

  private enum SectionType {
    NONE,
    COLUMN,
    ARGUMENT
  }
}
