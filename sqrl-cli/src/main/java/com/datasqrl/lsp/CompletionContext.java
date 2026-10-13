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
package com.datasqrl.lsp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * What can be completed at a position in a script. It is determined from the current text of the
 * statement at the position, so it works while the statement is being typed.
 *
 * @param kind what is completed
 * @param prefix the partially typed name before the position
 * @param path the qualifier of the name, e.g. {@code [sources]} for {@code sources.Ord}
 * @param table the table whose columns are completed, for {@link Kind#COLUMN} and {@link
 *     Kind#HINT_COLUMN}
 * @param names the names to suggest, for {@link Kind#KEYWORD} and {@link Kind#PARAMETER}
 * @param sql the query to complete, for {@link Kind#QUERY}
 * @param sqlCursor the position in the query, for {@link Kind#QUERY}
 * @param testStatement whether the statement defines a test table
 */
record CompletionContext(
    Kind kind,
    String prefix,
    List<String> path,
    String table,
    List<String> names,
    String sql,
    int sqlCursor,
    boolean testStatement) {

  enum Kind {
    /** Nothing, e.g. in a comment or a string */
    NONE,
    /** The start of a statement */
    STATEMENT,
    /** One of the given keywords */
    KEYWORD,
    /** The name of a hint in a hint comment */
    HINT,
    /** One of the given names of tables or aliases, e.g. in a join hint of a query */
    NAME,
    /** A variable of a template, e.g. {@code {{variant}}}, also in strings and comments */
    TEMPLATE,
    /** The path of an IMPORT statement */
    IMPORT,
    /** The sink of an EXPORT statement */
    EXPORT_SINK,
    /** The name of a table */
    TABLE,
    /** A column of the given table */
    COLUMN,
    /**
     * A column of the given table in the arguments of a hint, which the compiler reads as written
     */
    HINT_COLUMN,
    /** A parameter of the function that is defined */
    PARAMETER,
    /** A query, which the compiler completes */
    QUERY
  }

  private static final int FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;
  private static final String NAME = "[\\w.`$*-]*";

  /** A line that starts a SQRL definition, as in SqlScriptStatementSplitter */
  private static final Pattern DEFINITION_LINE =
      Pattern.compile(
          "^\\s*[\\w`][\\w.`*-]*\\s*(\\(.*\\))?\\s*(RETURNS\\s*\\(.*\\)\\s*)?:=", FLAGS);

  /** Text with parentheses nested up to two levels, e.g. {@code CONCAT(LEFT(name, 3), '%')} */
  private static final String NESTED = "(?:[^()]|\\((?:[^()]|\\([^()]*\\))*\\))*";

  /** A definition, whose parameters can be computed with function calls */
  static final Pattern DEFINITION =
      Pattern.compile(
          "^\\s*(?<name>[\\w`][\\w.`*-]*)\\s*(?:\\((?<args>"
              + NESTED
              + ")\\))?\\s*"
              + "(?:RETURNS\\s*\\(.*?\\)\\s*)?:=",
          FLAGS);

  private static final Pattern STATEMENT_START = Pattern.compile("^\\s*(\\w*)$", FLAGS);
  private static final Pattern IMPORT = Pattern.compile("^\\s*IMPORT\\s+(" + NAME + ")$", FLAGS);
  private static final Pattern EXPORT_TABLE =
      Pattern.compile("^\\s*EXPORT\\s+(" + NAME + ")$", FLAGS);
  private static final Pattern EXPORT_TO =
      Pattern.compile("^\\s*EXPORT\\s+" + NAME + "\\s+(\\w*)$", FLAGS);
  private static final Pattern EXPORT_SINK =
      Pattern.compile("^\\s*EXPORT\\s+" + NAME + "\\s+TO\\s+(" + NAME + ")$", FLAGS);
  private static final Pattern CREATE_TABLE =
      Pattern.compile("^\\s*CREATE\\s+(?:TEMPORARY\\s+)?TABLE\\b", FLAGS);
  private static final Pattern FLINK_QUERY =
      Pattern.compile("^\\s*(?:SELECT|WITH|INSERT|CREATE\\s+(?:TEMPORARY\\s+)?VIEW)\\b", FLAGS);

  private static final Pattern BODY_START = Pattern.compile("^\\s*(\\w*)$", FLAGS);
  private static final Pattern SUBSCRIBE = Pattern.compile("^\\s*(SUBSCRIBE)\\b", FLAGS);
  private static final Pattern DISTINCT_TABLE =
      Pattern.compile("^\\s*DISTINCT\\s+(" + NAME + ")$", FLAGS);
  private static final Pattern DISTINCT_ON =
      Pattern.compile("^\\s*DISTINCT\\s+" + NAME + "\\s+(\\w*)$", FLAGS);
  private static final Pattern DISTINCT_COLUMNS =
      Pattern.compile("^\\s*DISTINCT\\s+(" + NAME + ")\\s+(?:ON|ORDER\\s+BY)\\b.*?(\\w*)$", FLAGS);
  private static final Pattern QUERY_START = Pattern.compile("^\\s*(?:SELECT|WITH|\\()", FLAGS);
  private static final Pattern PARENT_COLUMN = Pattern.compile("\\bthis\\.(\\w*)$", FLAGS);
  private static final Pattern PARAMETER = Pattern.compile("(?<![\\w:])[:@](\\w*)$", FLAGS);
  private static final Pattern VARIABLE = Pattern.compile("(?<=\\W)[:@](?:\\w+|`[^`]*`)");
  private static final Pattern TEST_HINT = Pattern.compile("/\\*\\+[^*]*\\btest\\b", FLAGS);
  private static final Pattern TRAILING_NAME = Pattern.compile("(\\w*)$");
  private static final Pattern TEMPLATE = Pattern.compile("\\{\\{[#^/]?\\s*([\\w.]*)$");

  static final List<String> DEFINITION_KEYWORDS =
      List.of("SELECT", "DISTINCT", "SUBSCRIBE", "WITH");

  static final CompletionContext NONE = of(Kind.NONE, "");

  private static CompletionContext of(Kind kind, String prefix) {
    return new CompletionContext(kind, prefix, List.of(), null, List.of(), null, 0, false);
  }

  /** The context at an offset in the text of a script. */
  static CompletionContext at(String text, int offset) {
    offset = Math.max(0, Math.min(offset, text.length()));
    var template = TEMPLATE.matcher(text.substring(text.lastIndexOf('\n', offset - 1) + 1, offset));
    if (template.find()) {
      return of(Kind.TEMPLATE, template.group(1));
    }
    var scan = new Scan(text, offset);
    if (scan.cursorInString || (scan.cursorInComment && scan.hintStart < 0)) {
      return NONE;
    }
    if (scan.hintStart >= 0) {
      var hintEnd = text.indexOf("*/", scan.hintStart);
      var beforeCursor = text.substring(scan.hintStart + 3, offset);
      var queryBefore = scan.statementBefore(scan.hintStart);
      if (!queryBefore.isBlank()) {
        // A hint of Flink in a query, e.g. after SELECT, with the rest of the query
        var queryAfter = hintEnd < 0 ? "" : scan.text(hintEnd + 2, scan.statementEnd(hintEnd));
        return HintArguments.inQuery(beforeCursor, queryBefore, queryAfter);
      }
      // The statement that the hint is for, without comments
      var statement = hintEnd < 0 ? "" : scan.text(hintEnd + 2, text.length());
      return HintArguments.at(beforeCursor, statement);
    }
    return new Statement(text, scan, offset).context();
  }

  /** Whether an offset is in a hint comment, e.g. {@code /*+query_by_all(id)}. */
  static boolean isInHint(String text, int offset) {
    offset = Math.max(0, Math.min(offset, text.length()));
    return new Scan(text, offset).hintStart >= 0;
  }

  /**
   * The end of the statement at an offset, before its {@code ;}. The context at the end has the
   * whole query of the statement.
   *
   * @param offset an offset in the statement or in the comments before it, e.g. its hints
   */
  static int statementEnd(String text, int offset) {
    offset = Math.max(0, Math.min(offset, text.length()));
    var scan = new Scan(text, offset);
    while (offset < text.length() && Character.isWhitespace(scan.code[offset])) {
      offset++;
    }
    return new Statement(text, scan, offset).end;
  }

  static String trailingName(String text) {
    var matcher = TRAILING_NAME.matcher(text);
    return matcher.find() ? matcher.group(1) : "";
  }

  /** Splits a typed path such as {@code shop.sources.Ord} into its qualifier and prefix. */
  private static CompletionContext withPath(Kind kind, String typed, boolean testStatement) {
    var parts = new ArrayList<>(Arrays.asList(typed.replace("`", "").split("\\.", -1)));
    var prefix = parts.remove(parts.size() - 1);
    return new CompletionContext(
        kind, prefix, List.copyOf(parts), null, List.of(), null, 0, testStatement);
  }

  /**
   * The text with comments and the content of string literals replaced by spaces, so that the
   * offsets stay the same, and whether the cursor is in a comment or string.
   */
  private static final class Scan {
    final char[] code;
    final List<Integer> delimiters = new ArrayList<>();
    boolean cursorInComment = false;
    boolean cursorInString = false;
    int hintStart = -1;

    Scan(String text, int offset) {
      code = text.toCharArray();
      var i = 0;
      while (i < text.length()) {
        var c = text.charAt(i);
        int end;
        if (text.startsWith("--", i)) {
          end = indexOrLength(text, "\n", i);
          blank(i, end);
          cursorInComment |= offset > i && offset <= end;
        } else if (text.startsWith("/*", i)) {
          var close = text.indexOf("*/", i + 2);
          end = close < 0 ? text.length() : close + 2;
          blank(i, end);
          if (offset > i && (offset < end || close < 0)) {
            cursorInComment = true;
            if (text.startsWith("/*+", i)) {
              hintStart = i;
            }
          }
        } else if (c == '\'') {
          var close = closingQuote(text, i);
          end = close < 0 ? text.length() : close + 1;
          blank(i + 1, close < 0 ? end : close);
          cursorInString |= offset > i && (close < 0 || offset <= close);
        } else if (c == '`') {
          end = indexOrLength(text, "`", i + 1);
          end = Math.min(text.length(), end + 1);
        } else {
          if (c == ';') {
            delimiters.add(i);
          }
          end = i + 1;
        }
        i = end;
      }
    }

    String text(int start, int end) {
      return new String(code, start, end - start);
    }

    /** The statement at an offset up to the offset, without comments. */
    String statementBefore(int offset) {
      var start = 0;
      for (var delimiter : delimiters) {
        if (delimiter < offset) {
          start = delimiter + 1;
        }
      }
      return text(start, offset);
    }

    /** The end of the statement at an offset, at its {@code ;} or the end of the text. */
    int statementEnd(int offset) {
      return delimiters.stream().filter(d -> d >= offset).findFirst().orElse(code.length);
    }

    private void blank(int start, int end) {
      for (var i = start; i < end; i++) {
        if (code[i] != '\n') {
          code[i] = ' ';
        }
      }
    }

    private static int indexOrLength(String text, String search, int from) {
      var index = text.indexOf(search, from);
      return index < 0 ? text.length() : index;
    }

    /** The quote that closes a string literal, or -1 if it is not closed. */
    private static int closingQuote(String text, int start) {
      var i = start + 1;
      while (i < text.length()) {
        if (text.charAt(i) == '\'') {
          if (i + 1 < text.length() && text.charAt(i + 1) == '\'') {
            i += 2;
            continue;
          }
          return i;
        }
        i++;
      }
      return -1;
    }
  }

  /** The statement at the cursor. */
  private static final class Statement {
    final String text;
    final Scan scan;
    final int offset;
    final int start;
    final int end;
    final int previousEnd;

    Statement(String text, Scan scan, int offset) {
      this.text = text;
      this.scan = scan;
      this.offset = offset;
      var lastDelimiter =
          scan.delimiters.stream().filter(d -> d < offset).reduce((a, b) -> b).orElse(-1);
      var nextDelimiter =
          scan.delimiters.stream().filter(d -> d >= offset).findFirst().orElse(text.length());
      // A definition starts a new statement even if the previous one is missing its `;`
      var statementStart = lastDelimiter + 1;
      var statementEnd = nextDelimiter;
      for (var lineStart = statementStart; lineStart < statementEnd; ) {
        var lineEnd = text.indexOf('\n', lineStart);
        lineEnd = lineEnd < 0 ? text.length() : lineEnd;
        if (DEFINITION_LINE.matcher(scan.text(lineStart, lineEnd)).find()) {
          if (lineStart <= offset) {
            statementStart = lineStart;
          } else if (lineStart > offset) {
            statementEnd = lineStart;
            break;
          }
        }
        lineStart = lineEnd + 1;
      }
      this.previousEnd = lastDelimiter + 1;
      this.start = statementStart;
      this.end = statementEnd;
    }

    CompletionContext context() {
      var before = scan.text(start, offset);
      if (CREATE_TABLE.matcher(before).find()) {
        return NONE;
      }
      Matcher matcher;
      if ((matcher = STATEMENT_START.matcher(before)).find()) {
        return of(Kind.STATEMENT, matcher.group(1));
      }
      if ((matcher = IMPORT.matcher(before)).find()) {
        return withPath(Kind.IMPORT, matcher.group(1), false);
      }
      if ((matcher = EXPORT_SINK.matcher(before)).find()) {
        return withPath(Kind.EXPORT_SINK, matcher.group(1), false);
      }
      if ((matcher = EXPORT_TO.matcher(before)).find()) {
        return keywords(matcher.group(1), List.of("TO"));
      }
      if ((matcher = EXPORT_TABLE.matcher(before)).find()) {
        return withPath(Kind.TABLE, matcher.group(1), false);
      }
      var definition = DEFINITION.matcher(before);
      if (definition.find()) {
        return definition(definition);
      }
      if (FLINK_QUERY.matcher(before).find()) {
        return query(start, scan.text(start, end), offset - start, false);
      }
      return NONE;
    }

    private CompletionContext definition(Matcher definition) {
      var name = definition.group("name").replace("`", "");
      var parent = name.contains(".") ? name.substring(0, name.lastIndexOf('.')) : null;
      var test =
          TEST_HINT.matcher(text.substring(previousEnd, start + definition.start("name"))).find();
      var bodyStart = start + definition.end();
      var body = scan.text(bodyStart, offset);
      Matcher matcher;
      if ((matcher = BODY_START.matcher(body)).find()) {
        if (parent != null && !matcher.group(1).isEmpty()) {
          // A column expression or the query of a relationship
          return new CompletionContext(
              Kind.COLUMN, matcher.group(1), List.of(), parent, List.of("SELECT"), null, 0, test);
        }
        return keywords(matcher.group(1), DEFINITION_KEYWORDS);
      }
      if ((matcher = SUBSCRIBE.matcher(body)).find()) {
        // Remove the keyword, so that the rest is a query
        Arrays.fill(scan.code, bodyStart + matcher.start(1), bodyStart + matcher.end(1), ' ');
        body = scan.text(bodyStart, offset);
      }
      if ((matcher = DISTINCT_TABLE.matcher(body)).find()) {
        return withPath(Kind.TABLE, matcher.group(1), test);
      }
      if ((matcher = DISTINCT_ON.matcher(body)).find()) {
        return keywords(matcher.group(1), List.of("ON"));
      }
      if ((matcher = DISTINCT_COLUMNS.matcher(body)).find()) {
        return new CompletionContext(
            Kind.COLUMN,
            matcher.group(2),
            List.of(),
            matcher.group(1).replace("`", ""),
            List.of(),
            null,
            0,
            test);
      }
      if (parent != null && (matcher = PARENT_COLUMN.matcher(body)).find()) {
        return new CompletionContext(
            Kind.COLUMN, matcher.group(1), List.of(), parent, List.of(), null, 0, test);
      }
      if ((matcher = PARAMETER.matcher(body)).find()) {
        return new CompletionContext(
            Kind.PARAMETER,
            matcher.group(1),
            List.of(),
            null,
            parameters(definition.group("args")),
            null,
            0,
            test);
      }
      if (QUERY_START.matcher(body).find()) {
        return query(bodyStart, scan.text(bodyStart, end), offset - bodyStart, test);
      }
      if (parent != null) {
        // A column expression is completed as a query of the table
        var prefix = "SELECT ";
        var sql = prefix + scan.text(bodyStart, end) + "\nFROM " + parent;
        return new CompletionContext(
            Kind.QUERY,
            trailingName(body),
            List.of(),
            null,
            List.of(),
            replaceVariables(sql),
            prefix.length() + offset - bodyStart,
            test);
      }
      return NONE;
    }

    private static CompletionContext query(int sqlStart, String sql, int cursor, boolean test) {
      return new CompletionContext(
          Kind.QUERY,
          trailingName(sql.substring(0, cursor)),
          List.of(),
          null,
          List.of(),
          replaceVariables(sql),
          cursor,
          test);
    }

    private static CompletionContext keywords(String prefix, List<String> keywords) {
      return new CompletionContext(Kind.KEYWORD, prefix, List.of(), null, keywords, null, 0, false);
    }

    /** The names of the parameters in a signature such as {@code id BIGINT, name STRING}. */
    private static List<String> parameters(String signature) {
      if (signature == null || signature.isBlank()) {
        return List.of();
      }
      // Split at the commas between parameters, not those of function calls
      return Arrays.stream(signature.split(",(?![^(]*\\))"))
          .map(String::strip)
          .filter(p -> !p.isEmpty())
          .map(p -> p.split("\\s+")[0].replace("`", ""))
          .toList();
    }

    /**
     * Replaces the parameters of functions (e.g. {@code :id}) with literals of the same length,
     * since the compiler cannot complete queries with parameters.
     */
    private static String replaceVariables(String sql) {
      var matcher = VARIABLE.matcher(sql);
      var result = new StringBuilder();
      while (matcher.find()) {
        matcher.appendReplacement(result, "0" + " ".repeat(matcher.group().length() - 1));
      }
      matcher.appendTail(result);
      return result.toString();
    }
  }
}
