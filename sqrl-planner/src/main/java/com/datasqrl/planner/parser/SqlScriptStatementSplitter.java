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
package com.datasqrl.planner.parser;

import com.datasqrl.error.ErrorLocation.FileLocation;
import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Takes a script and splits it into individual statements delimited by `;`. Also filters out line
 * comments and block comments while preserving SQL hints and docs.
 *
 * <p>Contains some additional utility methods for statement delimiter handling.
 *
 * <p>TODO: Should we re-use this in the Flink runner?
 *
 * @see <a
 *     href="https://github.com/apache/flink-kubernetes-operator/blob/main/examples/flink-sql-runner-example/src/main/java/org/apache/flink/examples/SqlRunner.java">Flink's
 *     SqlRunner</a>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SqlScriptStatementSplitter {

  private static final String STATEMENT_DELIMITER = ";"; // a statement should end with `;`
  private static final String LINE_DELIMITER = "\n";
  private static final char SINGLE_QUOTE = '\'';

  /**
   * Parses SQL statements from a script.
   *
   * @param script The SQL script content.
   * @return A list of individual SQL statements.
   */
  public static List<ParsedObject<String>> splitStatements(String script) {
    if (script.isBlank()) {
      throw new StatementParserException("Script is empty");
    }

    var statements = new ArrayList<ParsedObject<String>>();

    var formatted = formatEndOfSqlFile(script);

    StringBuilder current = null;
    var statementLineNo = 0;
    var lineNo = 0;
    var inStringLiteral = false;
    var inPreservedBlockComment = false;
    var inBlockComment = false;
    for (var line : formatted.split(LINE_DELIMITER)) {
      lineNo++;

      var parsedLine = parseLine(line, inStringLiteral, inPreservedBlockComment, inBlockComment);
      var rawLine = parsedLine.line();
      inStringLiteral = parsedLine.inStringLiteral();
      inPreservedBlockComment = parsedLine.inPreservedBlockComment();
      inBlockComment = parsedLine.inBlockComment();
      if (rawLine.isBlank()) {
        continue;
      }

      if (current == null) {
        statementLineNo = lineNo;
        current = new StringBuilder();
      }

      current.append(rawLine);
      current.append(LINE_DELIMITER);

      if (!inStringLiteral
          && !inPreservedBlockComment
          && !inBlockComment
          && rawLine.trim().endsWith(STATEMENT_DELIMITER)) {
        var fileLoc = new FileLocation(statementLineNo, 1);
        var parsedObj = new ParsedObject<>(current.toString(), fileLoc);
        statements.add(parsedObj);
        current = null;
      }
    }

    return statements;
  }

  public static FileLocation computeOffset(String statement, int position) {
    Preconditions.checkArgument(position >= 0 && position <= statement.length());
    int lineNo = 1, columnNo = 1;
    for (var i = 0; i < position; i++) {
      columnNo++;
      if (statement.charAt(i) == '\n') {
        lineNo++;
        columnNo = 1;
      }
    }
    return new FileLocation(lineNo, columnNo);
  }

  /**
   * Formats the SQL file content to ensure proper statement termination at the end.
   *
   * @param sqlScript The SQL file content.
   * @return Formatted SQL content.
   */
  public static String formatEndOfSqlFile(String sqlScript) {
    var trimmed = CharMatcher.whitespace().trimTrailingFrom(sqlScript);
    var formatted = new StringBuilder();
    formatted.append(trimmed);
    if (!trimmed.endsWith(STATEMENT_DELIMITER)) {
      formatted.append(STATEMENT_DELIMITER);
    }
    formatted.append(LINE_DELIMITER);
    return formatted.toString();
  }

  public static String removeStatementDelimiter(String statement) {
    if (statement.trim().endsWith(STATEMENT_DELIMITER)) {
      var idx = statement.lastIndexOf(STATEMENT_DELIMITER);
      return statement.substring(0, idx);
    }
    return statement;
  }

  public static String addStatementDelimiter(String statement) {
    if (statement.trim().endsWith(STATEMENT_DELIMITER)) {
      return statement;
    }
    return statement + STATEMENT_DELIMITER;
  }

  /**
   * Parses a single line, removing SQL line comments and regular block comments while preserving
   * comment markers in string literals, SQL hints, and doc comments.
   *
   * <p>The state arguments carry parser state from the previous line so multiline string literals
   * and block comments are handled correctly. Only single-quoted SQL string literals are
   * recognized; escaped quotes are handled using SQL's doubled quote syntax ({@code ''}).
   *
   * @param line the line to parse
   * @param inStringLiteral whether the previous line ended inside a single-quoted string literal
   * @param inPreservedBlockComment whether the previous line ended inside a doc comment or SQL hint
   * @param inBlockComment whether the previous line ended inside a regular block comment
   * @return the parsed line and updated parser state
   */
  private static ParsedLine parseLine(
      String line,
      boolean inStringLiteral,
      boolean inPreservedBlockComment,
      boolean inBlockComment) {
    var parsed = new StringBuilder();

    for (var i = 0; i < line.length(); i++) {
      if (inBlockComment) {
        if (endsBlockComment(line, i)) {
          inBlockComment = false;
          i++;
        }
        continue;
      }

      if (inPreservedBlockComment) {
        parsed.append(line.charAt(i));
        if (endsBlockComment(line, i)) {
          parsed.append(line.charAt(i + 1));
          inPreservedBlockComment = false;
          i++;
        }
        continue;
      }

      if (!inStringLiteral && startsBlockComment(line, i)) {
        if (startsDocComment(line, i) || startsHintComment(line, i)) {
          inPreservedBlockComment = true;
          parsed.append(line.charAt(i));
        } else {
          inBlockComment = true;
          i++;
        }
        continue;
      }

      var ch = line.charAt(i);

      if (ch == SINGLE_QUOTE) {
        parsed.append(ch);
        if (isEscapedSingleQuote(line, i, inStringLiteral)) {
          parsed.append(line.charAt(i + 1));
          i++;
          continue;
        }

        inStringLiteral = !inStringLiteral;
        continue;
      }

      if (!inStringLiteral && startsLineComment(line, i)) {
        break;
      }

      parsed.append(ch);
    }

    return new ParsedLine(
        parsed.toString(), inStringLiteral, inPreservedBlockComment, inBlockComment);
  }

  private static boolean isEscapedSingleQuote(String line, int pos, boolean inStringLiteral) {
    return inStringLiteral && pos + 1 < line.length() && line.charAt(pos + 1) == SINGLE_QUOTE;
  }

  private static boolean startsLineComment(String line, int pos) {
    return line.charAt(pos) == '-' && pos + 1 < line.length() && line.charAt(pos + 1) == '-';
  }

  private static boolean startsBlockComment(String text, int pos) {
    return text.charAt(pos) == '/' && pos + 1 < text.length() && text.charAt(pos + 1) == '*';
  }

  private static boolean startsDocComment(String line, int pos) {
    return line.charAt(pos) == '/'
        && pos + 2 < line.length()
        && line.charAt(pos + 1) == '*'
        && line.charAt(pos + 2) == '*';
  }

  private static boolean startsHintComment(String text, int pos) {
    return text.charAt(pos) == '/'
        && pos + 2 < text.length()
        && text.charAt(pos + 1) == '*'
        && text.charAt(pos + 2) == '+';
  }

  private static boolean endsBlockComment(String line, int pos) {
    return line.charAt(pos) == '*' && pos + 1 < line.length() && line.charAt(pos + 1) == '/';
  }

  private record ParsedLine(
      String line,
      boolean inStringLiteral,
      boolean inPreservedBlockComment,
      boolean inBlockComment) {}
}
