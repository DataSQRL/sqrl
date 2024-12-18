package com.datasqrl.flinkwrapper;

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
  public List<String> splitStatements(String script) {
    String formatted =
        formatEndOfSqlFile(script)
            .replaceAll(LINE_COMMENT_PATTERN, "");
    if (removeBlockComments) {
      formatted = formatted.replaceAll(BLOCK_COMMENT_PATTERN, "");
    }

    List<String> statements = new ArrayList<>();

    StringBuilder current = null;
    boolean statementSet = false;
    for (String line : formatted.split(LINE_DELIMITER)) {
      String trimmed = line.trim();
      if (trimmed.isBlank()) {
        continue;
      }
      if (current == null) {
        current = new StringBuilder();
      }
      if (trimmed.startsWith("EXECUTE STATEMENT SET")) {
        statementSet = true;
      }
      current.append(trimmed);
      current.append(LINE_DELIMITER);
      if (trimmed.endsWith(STATEMENT_DELIMITER)) {
        if (!statementSet || trimmed.equalsIgnoreCase("END;")) {
          statements.add(current.toString());
          current = null;
          statementSet = false;
        }
      }
    }
    return statements;
  }

  public int countLines(String statement) {
    int count = 0;
    for (char c : statement.toCharArray()) {
      if (c == '\n') {
        count++;
      }
    }
    return count;
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
