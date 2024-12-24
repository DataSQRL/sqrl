package com.datasqrl.flinkwrapper.parser;

import com.datasqrl.error.ErrorLocation.FileLocation;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Value;

/**
 * Takes a script and splits it into individual statements delimited by `;`.
 *
 * TODO: Should we re-use this in the Flink runner?
 *
 * @see <a href="https://github.com/apache/flink-kubernetes-operator/blob/main/examples/flink-sql-runner-example/src/main/java/org/apache/flink/examples/SqlRunner.java">Flink's SqlRunner</a>
 *
 */
@Value
@AllArgsConstructor
public class SqlScriptStatementSplitter {

  private static final String STATEMENT_DELIMITER = ";"; // a statement should end with `;`
  private static final String LINE_DELIMITER = "\n";

  private static final String LINE_COMMENT_PATTERN = "--.*";
  public static final String BLOCK_COMMENT_PATTERN = "((/\\*)+?[\\w\\W]+?(\\*/)+)";

  boolean removeBlockComments;

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
    String formatted =
        formatEndOfSqlFile(script)
            .replaceAll(LINE_COMMENT_PATTERN, "");
    if (removeBlockComments) {
      formatted = formatted.replaceAll(BLOCK_COMMENT_PATTERN, "");
    }

    List<ParsedObject<String>> statements = new ArrayList<>();

    StringBuilder current = null;
    int statementLineNo = 0;
    int lineNo = 0;
    for (String line : formatted.split(LINE_DELIMITER)) {
      lineNo++;
      if (line.isBlank()) continue;
      if (current==null) {
        statementLineNo = lineNo;
        current = new StringBuilder();
      }
      current.append(line);
      current.append(LINE_DELIMITER);
      if (line.endsWith(STATEMENT_DELIMITER)) {
        statements.add(new ParsedObject<>(current.toString(), new FileLocation(statementLineNo, 1)));
        current = null;
      }
    }
    return statements;
  }

  public static FileLocation computeOffset(String statement, int position) {
    Preconditions.checkArgument(position>=0 && position<=statement.length());
    int lineNo = 1, columnNo = 1;
    for (int i = 0; i < position; i++) {
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
    String trimmed = sqlScript.trim();
    StringBuilder formatted = new StringBuilder();
    formatted.append(trimmed);
    if (!trimmed.endsWith(STATEMENT_DELIMITER)) {
      formatted.append(STATEMENT_DELIMITER);
    }
    formatted.append(LINE_DELIMITER);
    return formatted.toString();
  }

}
