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
  private static final String RETURNS = "RETURNS";
  private static final char SINGLE_QUOTE = '\'';
  private static final char BACKTICK = '`';
  private static final char DOUBLE_QUOTE = '"';
  private static final char NO_QUOTE = '\0';

  /**
   * A statement that is missing its terminating {@code ;}.
   *
   * @param statementStart the location of the statement in the script
   * @param delimiterLocation the location where the {@code ;} is missing
   */
  public record MissingDelimiter(FileLocation statementStart, FileLocation delimiterLocation) {}

  /**
   * Parses SQL statements from a script.
   *
   * @param script The SQL script content.
   * @return A list of individual SQL statements.
   */
  public static List<ParsedObject<String>> splitStatements(String script) {
    return splitStatements(script, new ArrayList<>());
  }

  /**
   * Parses SQL statements from a script. A SQRL definition that follows SQL of the current
   * statement starts a new statement, so that a single missing {@code ;} does not merge two
   * statements; the statement before it is reported in {@code missingDelimiters}. Comments and line
   * breaks inside or around the definition's header do not matter.
   *
   * @param script The SQL script content.
   * @param missingDelimiters Collects the statements that are missing their {@code ;}.
   * @return A list of individual SQL statements.
   */
  public static List<ParsedObject<String>> splitStatements(
      String script, List<MissingDelimiter> missingDelimiters) {
    if (script.isBlank()) {
      throw new StatementParserException("Script is empty");
    }

    var statements = new ArrayList<ParsedObject<String>>();

    var formatted = formatEndOfSqlFile(script);

    StringBuilder current = null;
    // The current statement with its comments and quoted text blanked out
    StringBuilder code = null;
    DefinitionScanner definitions = null;
    FileLocation statementStart = null;
    var lineNo = 0;
    var openQuote = NO_QUOTE;
    var inPreservedBlockComment = false;
    var statementTerminated = false;
    var statementContainsSql = false;
    for (var line : formatted.split(LINE_DELIMITER)) {
      lineNo++;

      var parsedLine = parseLine(line, openQuote, inPreservedBlockComment);
      var rawLine = parsedLine.line();
      // The statement ended with `;` on a line that ended inside a comment, so the SQL of this line
      // starts the next statement
      var startsNextStatement = statementTerminated && parsedLine.containsSql();
      openQuote = parsedLine.openQuote();
      inPreservedBlockComment = parsedLine.inPreservedBlockComment();
      if (rawLine.isBlank()) {
        if (current != null) {
          current.append(LINE_DELIMITER);
          code.append(LINE_DELIMITER);
        }
        continue;
      }

      if (current == null) {
        statementStart = new FileLocation(lineNo, 1);
        current = new StringBuilder();
        code = new StringBuilder();
        definitions = new DefinitionScanner();
      }
      var lineStart = current.length();
      current.append(rawLine).append(LINE_DELIMITER);
      code.append(parsedLine.code()).append(LINE_DELIMITER);
      statementTerminated |= parsedLine.endsWithStatementDelimiter();
      statementContainsSql |= parsedLine.containsSql();

      var nextStatement =
          startsNextStatement
              ? lineStart + CharMatcher.whitespace().negate().indexIn(parsedLine.code())
              : definitions.nextDefinition(code);
      while (nextStatement >= 0) {
        // The statement ends with its last SQL; what follows, e.g. a hint, belongs to the next one
        var sqlEnd =
            CharMatcher.whitespace().negate().lastIndexIn(code.subSequence(0, nextStatement)) + 1;
        statements.add(
            new ParsedObject<>(
                addStatementDelimiter(current.substring(0, sqlEnd)) + LINE_DELIMITER,
                statementStart));
        if (code.charAt(sqlEnd - 1) != STATEMENT_DELIMITER.charAt(0)) {
          missingDelimiters.add(
              new MissingDelimiter(
                  statementStart, statementStart.add(computeOffset(current.toString(), sqlEnd))));
        }

        var nextStart = CharMatcher.whitespace().negate().indexIn(current, sqlEnd);
        statementStart = statementStart.add(computeOffset(current.toString(), nextStart));
        current = new StringBuilder(current.substring(nextStart));
        code = new StringBuilder(code.substring(nextStart));
        definitions = new DefinitionScanner();
        statementTerminated = parsedLine.endsWithStatementDelimiter();
        nextStatement = definitions.nextDefinition(code);
      }

      if (openQuote == NO_QUOTE && !inPreservedBlockComment && statementTerminated) {
        var parsedObj = new ParsedObject<>(current.toString(), statementStart);
        statements.add(parsedObj);
        current = null;
        code = null;
        definitions = null;
        statementTerminated = false;
        statementContainsSql = false;
      }
    }

    // Emit an unterminated last statement so the SQL parser can report it; skip trailing comments.
    if (current != null && (statementContainsSql || inPreservedBlockComment)) {
      var statement =
          addStatementDelimiter(CharMatcher.whitespace().trimTrailingFrom(current))
              + LINE_DELIMITER;
      statements.add(new ParsedObject<>(statement, statementStart));
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
    var code = new StringBuilder();
    var lastSignificantCharacter = '\0';

    for (var i = 0; i < line.length(); i++) {
      if (inPreservedBlockComment) {
        parsed.append(line.charAt(i));
        code.append(' ');
        if (endsBlockComment(line, i)) {
          parsed.append(line.charAt(i + 1));
          code.append(' ');
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
          code.append(ch);
          openQuote = NO_QUOTE;
          lastSignificantCharacter = ch;
        } else {
          // Quoted text is not code, but a name in backticks stays a name
          code.append(openQuote == BACKTICK ? '_' : ' ');
        }
        continue;
      }

      if (startsBlockComment(line, i)) {
        inPreservedBlockComment = true;
        parsed.append(ch);
        code.append(' ');
        continue;
      }

      if (startsLineComment(line, i)) {
        break;
      }

      parsed.append(ch);
      code.append(ch);
      if (ch == SINGLE_QUOTE || ch == BACKTICK || ch == DOUBLE_QUOTE) {
        openQuote = ch;
      }
      if (!Character.isWhitespace(ch)) {
        lastSignificantCharacter = ch;
      }
    }

    return new ParsedLine(
        parsed.toString(),
        code.toString(),
        openQuote,
        inPreservedBlockComment,
        lastSignificantCharacter == STATEMENT_DELIMITER.charAt(0),
        lastSignificantCharacter != '\0');
  }

  /**
   * Finds the start of the header of the definition whose {@code :=} is at {@code operator}. The
   * header is {@code name [(arguments)] [RETURNS (columns)]}, as {@code SqrlStatementParser} reads
   * it, so it is read here backwards from the {@code :=}, one name or group at a time.
   *
   * @return where the header starts, or -1 if there is no header before the {@code :=}
   */
  private static int headerStart(CharSequence code, int operator) {
    var last = tokenBefore(code, operator);
    if (last == null || !last.group()) {
      // name :=
      return last == null ? -1 : last.start();
    }

    var name = tokenBefore(code, last.start());
    if (name == null || name.group()) {
      return -1;
    }

    if (RETURNS.equalsIgnoreCase(name.text(code))) {
      // name [(arguments)] RETURNS (columns) :=, unless no name comes before RETURNS, or RETURNS
      // starts a line that the name before it does not, e.g. `FROM T` and then `Returns(x INT) :=`
      var function = nameBefore(code, name.start());
      if (function != null
          && (startsLine(code, function.start()) || !startsLine(code, name.start()))) {
        return function.start();
      }
    }

    // name (arguments) :=
    return name.start();
  }

  /** The name that ends before {@code end}, optionally followed by its arguments, or null. */
  private static Token nameBefore(CharSequence code, int end) {
    var token = tokenBefore(code, end);
    if (token != null && token.group()) {
      token = tokenBefore(code, token.start());
    }

    return token == null || token.group() ? null : token;
  }

  /**
   * The name, or the group in parentheses, that ends before {@code end} after whitespace, or null
   * if there is neither.
   */
  private static Token tokenBefore(CharSequence code, int end) {
    var last = end - 1;
    while (last >= 0 && Character.isWhitespace(code.charAt(last))) {
      last--;
    }

    if (last < 0) {
      return null;
    }

    if (code.charAt(last) == ')') {
      var depth = 0;
      for (var i = last; i >= 0; i--) {
        if (code.charAt(i) == ')') {
          depth++;
        } else if (code.charAt(i) == '(' && --depth == 0) {
          return new Token(i, last + 1, true);
        }
      }
      return null;
    }

    var start = last + 1;
    while (start > 0 && isNameCharacter(code.charAt(start - 1))) {
      start--;
    }

    return start <= last ? new Token(start, last + 1, false) : null;
  }

  private static boolean isNameCharacter(char ch) {
    return Character.isLetterOrDigit(ch) || "_.*-`".indexOf(ch) >= 0;
  }

  /** Whether only whitespace comes before {@code index} on its line. */
  private static boolean startsLine(CharSequence code, int index) {
    for (var i = index - 1; i >= 0 && code.charAt(i) != '\n'; i--) {
      if (!Character.isWhitespace(code.charAt(i))) {
        return false;
      }
    }
    return true;
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

  /**
   * Scans the code of a statement for SQRL definitions while its lines are added, continuing where
   * the last scan stopped, so that each character is scanned once.
   */
  private static final class DefinitionScanner {

    private int position = 0;
    // The parentheses that are open at the position
    private int depth = 0;

    /**
     * Finds a SQRL definition that does not start the statement: a {@code :=} outside of
     * parentheses, where it is not valid SQL, with SQL before its header. Inside parentheses,
     * {@code :=} can be a named argument of a function in the SQL of a database, e.g. {@code p_id
     * := :id}.
     *
     * @param code the statement without its comments and quoted text
     * @return where the header of the definition starts, or -1 if there is no such definition
     */
    int nextDefinition(CharSequence code) {
      for (; position + 1 < code.length(); position++) {
        var ch = code.charAt(position);
        if (ch == '(') {
          depth++;
        } else if (ch == ')') {
          depth--;
        } else if (ch == ':' && code.charAt(position + 1) == '=' && depth <= 0) {
          var header = headerStart(code, position);
          if (header > 0 && !CharMatcher.whitespace().matchesAllOf(code.subSequence(0, header))) {
            return header;
          }
        }
      }
      return -1;
    }
  }

  /**
   * A line without its line comment, and the state after it.
   *
   * @param code the line with its block comments and quoted text replaced by spaces, so that the
   *     columns stay the same; the quote characters are kept, and the text of a name in backticks
   *     is replaced by {@code _}
   */
  private record ParsedLine(
      String line,
      String code,
      char openQuote,
      boolean inPreservedBlockComment,
      boolean endsWithStatementDelimiter,
      boolean containsSql) {}

  /**
   * A name, or a group in parentheses, in the code of a statement.
   *
   * @param start where it starts
   * @param end where it ends, exclusive
   * @param group whether it is a group in parentheses
   */
  private record Token(int start, int end, boolean group) {

    String text(CharSequence code) {
      return code.subSequence(start, end).toString();
    }
  }
}
