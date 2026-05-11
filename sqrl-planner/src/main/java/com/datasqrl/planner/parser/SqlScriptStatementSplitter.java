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
import java.util.regex.Pattern;
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

  // Matches block comments that are NOT hints or doc comments
  private static final Pattern BLOCK_COMMENT_PATTERN =
      Pattern.compile("/\\*(?!\\s*\\+)(?!\\*)[\\s\\S]*?\\*/");

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
    formatted = removeBlockComments(formatted);

    StringBuilder current = null;
    var statementLineNo = 0;
    var lineNo = 0;
    var inStringLiteral = false;
    for (var line : formatted.split(LINE_DELIMITER)) {
      lineNo++;

      var lineCommentResult = removeLineComment(line, inStringLiteral);
      var rawLine = lineCommentResult.line();
      inStringLiteral = lineCommentResult.inStringLiteral();
      if (rawLine.isBlank()) {
        continue;
      }

      if (current == null) {
        statementLineNo = lineNo;
        current = new StringBuilder();
      }

      current.append(rawLine);
      current.append(LINE_DELIMITER);

      if (rawLine.trim().endsWith(STATEMENT_DELIMITER)) {
        statements.add(
            new ParsedObject<>(current.toString(), new FileLocation(statementLineNo, 1)));
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
   * Removes a SQL line comment from a single line while preserving {@code --} inside SQL string
   * literals.
   *
   * <p>The {@code inStringLiteral} argument carries parser state from the previous line so
   * multiline string literals are handled correctly. Only single-quoted SQL string literals are
   * recognized; escaped quotes are handled using SQL's doubled quote syntax ({@code ''}).
   *
   * @param line the line to scan
   * @param inStringLiteral whether the previous line ended inside a single-quoted string literal
   * @return the line without its trailing comment and the updated string-literal state
   */
  private static LineCommentResult removeLineComment(String line, boolean inStringLiteral) {
    var lineEnd = line.length();

    for (var i = 0; i < line.length(); i++) {
      var ch = line.charAt(i);

      if (ch == SINGLE_QUOTE) {
        if (isEscapedSingleQuote(line, i, inStringLiteral)) {
          i++;
          continue;
        }

        inStringLiteral = !inStringLiteral;
        continue;
      }

      if (!inStringLiteral && startsLineComment(line, i)) {
        lineEnd = i;
        break;
      }
    }

    return new LineCommentResult(line.substring(0, lineEnd), inStringLiteral);
  }

  private static boolean isEscapedSingleQuote(String line, int pos, boolean inStringLiteral) {
    return inStringLiteral && pos + 1 < line.length() && line.charAt(pos + 1) == SINGLE_QUOTE;
  }

  private static boolean startsLineComment(String line, int pos) {
    return line.charAt(pos) == '-' && pos + 1 < line.length() && line.charAt(pos + 1) == '-';
  }

  /**
   * Removes block comments from SQL script while preserving SQL hints and doc comments.
   *
   * <p>Newlines within removed comments are preserved as blank lines to maintain accurate line
   * numbering for error reporting.
   *
   * @param text The SQL script text.
   * @return The text with regular block comments removed.
   */
  private static String removeBlockComments(String text) {
    var matcher = BLOCK_COMMENT_PATTERN.matcher(text);
    var result = new StringBuilder();
    var lastEnd = 0;

    while (matcher.find()) {
      result.append(text, lastEnd, matcher.start());
      // Count newlines in the comment and preserve them as blank lines
      var comment = matcher.group();
      var newlineCount = comment.chars().filter(ch -> ch == '\n').count();
      result.append(LINE_DELIMITER.repeat((int) newlineCount));
      lastEnd = matcher.end();
    }
    result.append(text.substring(lastEnd));

    return result.toString();
  }

  private record LineCommentResult(String line, boolean inStringLiteral) {}
}
