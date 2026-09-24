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
 * comments while preserving block comments.
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
  private static final char BACKTICK = '`';
  private static final char DOUBLE_QUOTE = '"';
  private static final char NO_QUOTE = '\0';

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
    var openQuote = NO_QUOTE;
    var inPreservedBlockComment = false;
    var statementTerminated = false;
    var statementContainsSql = false;
    for (var line : formatted.split(LINE_DELIMITER)) {
      lineNo++;

      var parsedLine = parseLine(line, openQuote, inPreservedBlockComment);
      var rawLine = parsedLine.line();
      openQuote = parsedLine.openQuote();
      inPreservedBlockComment = parsedLine.inPreservedBlockComment();
      statementTerminated |= parsedLine.endsWithStatementDelimiter();
      statementContainsSql |= parsedLine.containsSql();
      if (rawLine.isBlank()) {
        if (current != null) {
          current.append(LINE_DELIMITER);
        }
        continue;
      }

      if (current == null) {
        statementLineNo = lineNo;
        current = new StringBuilder();
      }

      current.append(rawLine);
      current.append(LINE_DELIMITER);

      if (openQuote == NO_QUOTE && !inPreservedBlockComment && statementTerminated) {
        var fileLoc = new FileLocation(statementLineNo, 1);
        var parsedObj = new ParsedObject<>(current.toString(), fileLoc);
        statements.add(parsedObj);
        current = null;
        statementTerminated = false;
        statementContainsSql = false;
      }
    }

    // Emit an unterminated last statement so the SQL parser can report it; skip trailing comments.
    if (current != null && (statementContainsSql || inPreservedBlockComment)) {
      var statement =
          addStatementDelimiter(CharMatcher.whitespace().trimTrailingFrom(current))
              + LINE_DELIMITER;
      statements.add(new ParsedObject<>(statement, new FileLocation(statementLineNo, 1)));
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
   * Parses a single line, removing SQL line comments while preserving block comments and comment
   * markers in string literals and quoted identifiers.
   *
   * <p>The state arguments carry parser state from the previous line so multiline quoted sections
   * and block comments are handled correctly. Single-quoted SQL string literals, backtick-quoted
   * Flink identifiers, and double-quoted database identifiers (used in pass-through queries) are
   * recognized; escaped quotes are handled using SQL's doubled quote syntax (e.g. {@code ''}).
   *
   * @param line the line to parse
   * @param openQuote the quote character of the section the previous line ended in, or {@link
   *     #NO_QUOTE}
   * @param inPreservedBlockComment whether the previous line ended inside a block comment
   * @return the parsed line and updated parser state
   */
  private static ParsedLine parseLine(
      String line, char openQuote, boolean inPreservedBlockComment) {
    var parsed = new StringBuilder();
    var lastSignificantCharacter = '\0';

    for (var i = 0; i < line.length(); i++) {
      if (inPreservedBlockComment) {
        parsed.append(line.charAt(i));
        if (endsBlockComment(line, i)) {
          parsed.append(line.charAt(i + 1));
          inPreservedBlockComment = false;
          i++;
        }
        continue;
      }

      var ch = line.charAt(i);

      // Quoted content never starts a comment or ends a statement. Doubled quotes (e.g. '')
      // need no special handling, since they close and immediately reopen the quoted section.
      if (openQuote != NO_QUOTE) {
        parsed.append(ch);
        if (ch == openQuote) {
          openQuote = NO_QUOTE;
          lastSignificantCharacter = ch;
        }
        continue;
      }

      if (startsBlockComment(line, i)) {
        inPreservedBlockComment = true;
        parsed.append(ch);
        continue;
      }

      if (startsLineComment(line, i)) {
        break;
      }

      parsed.append(ch);
      if (ch == SINGLE_QUOTE || ch == BACKTICK || ch == DOUBLE_QUOTE) {
        openQuote = ch;
      }
      if (!Character.isWhitespace(ch)) {
        lastSignificantCharacter = ch;
      }
    }

    return new ParsedLine(
        parsed.toString(),
        openQuote,
        inPreservedBlockComment,
        lastSignificantCharacter == STATEMENT_DELIMITER.charAt(0),
        lastSignificantCharacter != '\0');
  }

  private static boolean startsLineComment(String line, int pos) {
    return line.charAt(pos) == '-' && pos + 1 < line.length() && line.charAt(pos + 1) == '-';
  }

  private static boolean startsBlockComment(String text, int pos) {
    return text.charAt(pos) == '/' && pos + 1 < text.length() && text.charAt(pos + 1) == '*';
  }

  private static boolean endsBlockComment(String line, int pos) {
    return line.charAt(pos) == '*' && pos + 1 < line.length() && line.charAt(pos + 1) == '/';
  }

  private record ParsedLine(
      String line,
      char openQuote,
      boolean inPreservedBlockComment,
      boolean endsWithStatementDelimiter,
      boolean containsSql) {}
}
