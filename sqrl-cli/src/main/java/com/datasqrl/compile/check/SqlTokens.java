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
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Splits SQL in the dialect of a database into names, punctuation and everything else, skipping
 * comments. Names may be quoted with double quotes and qualified, e.g. {@code r."managerid"}.
 */
final class SqlTokens {

  enum Kind {
    /** A name, which may be qualified, e.g. {@code r."managerid"} */
    NAME,
    /** One of {@code ( ) , ;} */
    PUNCTUATION,
    /** Anything else, e.g. a literal, a parameter or an operator */
    OTHER
  }

  /**
   * An element of the SQL.
   *
   * @param kind what it is
   * @param parts the parts of a name, without quotes
   * @param keyword the lower-case keyword of a single unquoted name, or else null
   * @param text the text of punctuation or of something else
   */
  record Element(Kind kind, List<String> parts, String keyword, String text, int start, int end) {

    boolean is(String word) {
      return word.equals(keyword);
    }

    boolean isAny(Set<String> words) {
      return keyword != null && words.contains(keyword);
    }

    boolean isPunctuation(String punctuation) {
      return kind == Kind.PUNCTUATION && text.equals(punctuation);
    }
  }

  private SqlTokens() {}

  /** Splits the SQL into names, punctuation and everything else, skipping comments. */
  static List<Element> tokenize(String sql) {
    var result = new ArrayList<Element>();
    var i = 0;
    while (i < sql.length()) {
      var c = sql.charAt(i);
      if (Character.isWhitespace(c)) {
        i++;
      } else if (sql.startsWith("--", i)) {
        var end = sql.indexOf('\n', i);
        i = end < 0 ? sql.length() : end + 1;
      } else if (sql.startsWith("/*", i)) {
        var end = sql.indexOf("*/", i + 2);
        i = end < 0 ? sql.length() : end + 2;
      } else if (c == '\'') {
        var end = quoted(sql, i);
        result.add(new Element(Kind.OTHER, List.of(), null, sql.substring(i, end), i, end));
        i = end;
      } else if (c == '"' || Character.isLetter(c) || c == '_') {
        i = name(sql, i, result);
      } else if ("(),;".indexOf(c) >= 0) {
        result.add(new Element(Kind.PUNCTUATION, List.of(), null, String.valueOf(c), i, i + 1));
        i++;
      } else if (sql.startsWith("::", i)) {
        // A cast, whose type is not a column
        var end = i + 2;
        while (end < sql.length() && Character.isWhitespace(sql.charAt(end))) {
          end++;
        }
        while (end < sql.length()
            && (Character.isLetterOrDigit(sql.charAt(end)) || sql.charAt(end) == '_')) {
          end++;
        }
        result.add(new Element(Kind.OTHER, List.of(), null, sql.substring(i, end), i, end));
        i = end;
      } else {
        var end = i + 1;
        if (c == ':' || c == '$' || Character.isDigit(c)) {
          // A parameter, e.g. :customerid, or a number
          while (end < sql.length()
              && (Character.isLetterOrDigit(sql.charAt(end))
                  || sql.charAt(end) == '_'
                  || sql.charAt(end) == '.')) {
            end++;
          }
        }
        result.add(new Element(Kind.OTHER, List.of(), null, sql.substring(i, end), i, end));
        i = end;
      }
    }
    return result;
  }

  /** Reads a name that may be qualified, and returns the offset after it. */
  private static int name(String sql, int start, List<Element> result) {
    var parts = new ArrayList<String>();
    var i = start;
    var quotedPart = false;
    while (i < sql.length()) {
      if (sql.charAt(i) == '"') {
        var end = quoted(sql, i);
        parts.add(sql.substring(i + 1, Math.max(i + 1, end - 1)).replace("\"\"", "\""));
        quotedPart = true;
        i = end;
      } else {
        var end = i;
        while (end < sql.length()
            && (Character.isLetterOrDigit(sql.charAt(end))
                || sql.charAt(end) == '_'
                || sql.charAt(end) == '$')) {
          end++;
        }
        if (end == i) {
          break;
        }
        parts.add(sql.substring(i, end));
        quotedPart = false;
        i = end;
      }
      if (i + 1 < sql.length()
          && sql.charAt(i) == '.'
          && (sql.charAt(i + 1) == '"'
              || Character.isLetter(sql.charAt(i + 1))
              || sql.charAt(i + 1) == '_')) {
        i++;
      } else {
        break;
      }
    }
    var keyword = parts.size() == 1 && !quotedPart ? parts.get(0).toLowerCase(Locale.ROOT) : null;
    result.add(new Element(Kind.NAME, List.copyOf(parts), keyword, "", start, i));
    return i;
  }

  /** The offset after a quoted text that starts at an offset, where doubled quotes are escaped. */
  private static int quoted(String sql, int start) {
    var quote = sql.charAt(start);
    var i = start + 1;
    while (i < sql.length()) {
      if (sql.charAt(i) == quote) {
        if (i + 1 < sql.length() && sql.charAt(i + 1) == quote) {
          i += 2;
          continue;
        }
        return i + 1;
      }
      i++;
    }
    return sql.length();
  }
}
