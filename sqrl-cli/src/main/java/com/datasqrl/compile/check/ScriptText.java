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
package com.datasqrl.compile.check;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.IntPredicate;

/**
 * The text of a script with conversions between offsets and 0-based line and character positions,
 * and lookups of (possibly qualified) names such as {@code sources.Orders} or {@code `my table`}.
 */
public final class ScriptText {

  /**
   * A part of a qualified name, e.g. {@code Orders} in {@code sources.Orders}, without quotes.
   *
   * @param name the name
   * @param start the offset where the part starts, including a quote
   * @param end the offset after the part, including a quote
   */
  public record NamePart(String name, int start, int end) {}

  private final String text;
  private final int[] lineStarts;

  public ScriptText(String text) {
    this.text = text;
    var starts = new ArrayList<Integer>();
    starts.add(0);
    for (var i = 0; i < text.length(); i++) {
      if (text.charAt(i) == '\n') {
        starts.add(i + 1);
      }
    }
    this.lineStarts = starts.stream().mapToInt(Integer::intValue).toArray();
  }

  public String text() {
    return text;
  }

  public int lineCount() {
    return lineStarts.length;
  }

  /** A line without its line break. */
  public String line(int line) {
    if (line < 0 || line >= lineStarts.length) {
      return "";
    }
    var end = line + 1 < lineStarts.length ? lineStarts[line + 1] - 1 : text.length();
    if (end > lineStarts[line] && text.charAt(end - 1) == '\r') {
      end--;
    }
    return text.substring(lineStarts[line], end);
  }

  /** The offset of a 0-based position, clamped to the text. */
  public int offset(int line, int character) {
    if (line < 0) {
      return 0;
    }
    if (line >= lineStarts.length) {
      return text.length();
    }
    return lineStarts[line] + Math.max(0, Math.min(character, line(line).length()));
  }

  /** The 0-based line of an offset. */
  public int lineOf(int offset) {
    var index = Arrays.binarySearch(lineStarts, offset);
    return index >= 0 ? index : Math.max(0, -index - 2);
  }

  /** The 0-based character of an offset in its line. */
  public int characterOf(int offset) {
    return offset - lineStarts[lineOf(offset)];
  }

  /**
   * Finds the first occurrence of a name at or after an offset, skipping comments and string
   * literals, up to the end of the statement at the next {@code ;}. A name matches the trailing
   * parts of a qualified name in the text, case-insensitively.
   *
   * @param name the name, may be qualified, e.g. {@code Customers.orders}
   * @param from the offset to start at
   * @return the parts of the matching name in the text
   */
  public Optional<List<NamePart>> find(String name, int from) {
    var wanted = List.of(name.split("\\."));
    var i = Math.max(0, from);
    while (i < text.length()) {
      var c = text.charAt(i);
      if (c == ';') {
        return Optional.empty();
      } else if (startsWith("--", i)) {
        i = skipTo("\n", i);
      } else if (startsWith("/*", i)) {
        i = skipTo("*/", i + 2);
      } else if (c == '\'') {
        i = skipString(i);
      } else if (isNameStart(c)) {
        var parts = readName(i);
        if (parts.isEmpty()) {
          i++;
          continue;
        }
        if (endsWith(parts, wanted)) {
          return Optional.of(parts.subList(parts.size() - wanted.size(), parts.size()));
        }
        i = parts.get(parts.size() - 1).end();
      } else {
        i++;
      }
    }
    return Optional.empty();
  }

  /**
   * The qualified name at an offset, up to and including the part at the offset, e.g. {@code
   * [sources, Orders]} for an offset in {@code Orders} of {@code sources.Orders.id}.
   */
  public Optional<List<NamePart>> nameAt(int offset) {
    var start = offset;
    while (start > 0 && isNameChar(text.charAt(start - 1))) {
      start--;
    }
    while (start < offset && !isNameStart(text.charAt(start))) {
      start++;
    }
    if (start >= text.length() || !isNameStart(text.charAt(start))) {
      return Optional.empty();
    }
    var parts = readName(start);
    var upToOffset = new ArrayList<NamePart>();
    for (var part : parts) {
      upToOffset.add(part);
      if (part.end() >= offset) {
        break;
      }
    }
    if (upToOffset.isEmpty() || upToOffset.get(upToOffset.size() - 1).end() < offset) {
      return Optional.empty();
    }
    return Optional.of(upToOffset);
  }

  /** The qualified name that starts at an offset, or an empty list if there is none. */
  public List<NamePart> nameStartingAt(int offset) {
    if (offset < 0 || offset >= text.length() || !isNameStart(text.charAt(offset))) {
      return List.of();
    }
    return readName(offset);
  }

  /**
   * The path of an IMPORT or EXPORT statement that starts at an offset, or an empty list if there
   * is none. Unlike in names of SQL, its parts can contain {@code -}, e.g. {@code
   * cluster-events.metrics}.
   */
  public List<NamePart> pathStartingAt(int offset) {
    if (offset < 0 || offset >= text.length() || !isNameStart(text.charAt(offset))) {
      return List.of();
    }
    return readName(offset, c -> isWordChar((char) c) || c == '-');
  }

  /**
   * Splits a qualified name into its parts, or returns empty if the text is not a single qualified
   * name.
   */
  public static Optional<List<NamePart>> parseName(String name) {
    var script = new ScriptText(name);
    if (name.isEmpty() || !isNameStart(name.charAt(0))) {
      return Optional.empty();
    }
    var parts = script.readName(0);
    if (parts.isEmpty() || parts.get(parts.size() - 1).end() != name.length()) {
      return Optional.empty();
    }
    return Optional.of(parts);
  }

  /** Whether the names of the parts end with the given names, ignoring case. */
  public static boolean endsWith(List<NamePart> parts, List<String> names) {
    if (names.isEmpty() || parts.size() < names.size()) {
      return false;
    }
    var offset = parts.size() - names.size();
    for (var i = 0; i < names.size(); i++) {
      if (!parts.get(offset + i).name().equalsIgnoreCase(names.get(i))) {
        return false;
      }
    }
    return true;
  }

  private List<NamePart> readName(int start) {
    return readName(start, c -> isWordChar((char) c));
  }

  private List<NamePart> readName(int start, IntPredicate isPartChar) {
    var parts = new ArrayList<NamePart>();
    var i = start;
    while (i < text.length()) {
      int end;
      String name;
      if (isQuote(text.charAt(i))) {
        end = text.indexOf(text.charAt(i), i + 1);
        if (end < 0) {
          break;
        }
        name = text.substring(i + 1, end);
        end++;
      } else if (isWordChar(text.charAt(i))) {
        end = i;
        while (end < text.length() && isPartChar.test(text.charAt(end))) {
          end++;
        }
        name = text.substring(i, end);
      } else {
        break;
      }
      parts.add(new NamePart(name, i, end));
      if (end + 1 < text.length() && text.charAt(end) == '.' && isNameStart(text.charAt(end + 1))) {
        i = end + 1;
      } else {
        break;
      }
    }
    return parts;
  }

  private boolean startsWith(String prefix, int offset) {
    return text.startsWith(prefix, offset);
  }

  private int skipTo(String end, int from) {
    var index = text.indexOf(end, from);
    return index < 0 ? text.length() : index + end.length();
  }

  private int skipString(int start) {
    var i = start + 1;
    while (i < text.length()) {
      if (text.charAt(i) == '\'') {
        if (i + 1 < text.length() && text.charAt(i + 1) == '\'') {
          i += 2;
          continue;
        }
        return i + 1;
      }
      i++;
    }
    return text.length();
  }

  private static boolean isNameStart(char c) {
    return isQuote(c) || isWordChar(c);
  }

  /** Names are quoted with backticks, or with double quotes in the SQL of a database. */
  private static boolean isQuote(char c) {
    return c == '`' || c == '"';
  }

  private static boolean isNameChar(char c) {
    return isNameStart(c) || c == '.';
  }

  private static boolean isWordChar(char c) {
    return Character.isLetterOrDigit(c) || c == '_' || c == '$';
  }
}
