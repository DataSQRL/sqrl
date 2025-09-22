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
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;

/**
 * Takes a script and splits it into individual statements delimited by `;`. Also filters out `--`
 * comments
 *
 * <p>Contains some additional utility methods for statement delimiter handling.
 *
 * <p>TODO: Should we re-use this in the Flink runner?
 *
 * @see <a
 *     href="https://github.com/apache/flink-kubernetes-operator/blob/main/examples/flink-sql-runner-example/src/main/java/org/apache/flink/examples/SqlRunner.java">Flink's
 *     SqlRunner</a>
 */
public record SqlScriptStatementSplitter(boolean removeBlockComments) {

  public static final String STATEMENT_DELIMITER = ";"; // a statement should end with `;`
  public static final String LINE_DELIMITER = "\n";

  private static final String LINE_COMMENT_PATTERN = "--.*";
  public static final String BLOCK_COMMENT_PATTERN = "((/\\*)+?[\\w\\W]+?(\\*/)+)";

  public SqlScriptStatementSplitter() {
    this(false);
  }

  /**
   * Parses SQL statements from a script.
   *
   * @param script The SQL script content.
   * @return A list of individual SQL statements.
   */
  public List<ParsedObject<String>> splitStatements(String script) {
    if (script.isBlank()) {
      throw new StatementParserException("Script is empty");
    }
    var formatted = formatEndOfSqlFile(script).replaceAll(LINE_COMMENT_PATTERN, "");
    if (removeBlockComments) {
      formatted = formatted.replaceAll(BLOCK_COMMENT_PATTERN, "");
    }

    List<ParsedObject<String>> statements = new ArrayList<>();

    StringBuilder current = null;
    var statementLineNo = 0;
    var lineNo = 0;
    for (String line : formatted.split(LINE_DELIMITER)) {
      lineNo++;
      if (line.isBlank()) {
        continue;
      }
      if (current == null) {
        statementLineNo = lineNo;
        current = new StringBuilder();
      }
      current.append(line);
      current.append(LINE_DELIMITER);
      if (line.trim().endsWith(STATEMENT_DELIMITER)) {
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
    var trimmed = sqlScript.trim();
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
}
