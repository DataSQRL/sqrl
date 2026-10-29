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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import org.eclipse.lsp4j.FormattingOptions;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.Range;
import org.eclipse.lsp4j.TextEdit;

/**
 * Formats the layout of a script: the indentation of its lines from the structure of its queries,
 * and the spaces between tokens. It keeps line breaks, comments, literals, templates and the case
 * of names, since names become the fields of the API, and only changes whitespace: a result whose
 * tokens differ from the script's is discarded.
 */
public class ScriptFormatter {

  private static final Set<String> CLAUSES =
      Set.of(
          "SELECT",
          "FROM",
          "WHERE",
          "GROUP",
          "HAVING",
          "ORDER",
          "LIMIT",
          "OFFSET",
          "FETCH",
          "UNION",
          "INTERSECT",
          "EXCEPT",
          "MINUS",
          "WINDOW",
          "QUALIFY",
          "JOIN",
          "INNER",
          "CROSS",
          "NATURAL",
          "VALUES",
          "PARTITION",
          "WITH");

  /** Clauses whose conditions are indented, also when they are in parentheses */
  private static final Set<String> CONDITIONS = Set.of("WHERE", "HAVING", "QUALIFY");

  /** Clause keywords that are also functions, e.g. LEFT(name, 3), unless a join follows */
  private static final Set<String> JOIN_SIDES = Set.of("LEFT", "RIGHT", "FULL");

  private static final Set<String> TYPES_WITH_ARGUMENTS = Set.of("ARRAY", "MAP", "MULTISET", "ROW");

  /** Words after which a sign or `*` is unary or a wildcard rather than an operator */
  private static final Set<String> OPERAND_STARTS =
      Set.of(
          "SELECT",
          "WHERE",
          "AND",
          "OR",
          "NOT",
          "WHEN",
          "THEN",
          "ELSE",
          "ON",
          "BY",
          "IN",
          "IS",
          "BETWEEN",
          "CASE",
          "RETURN",
          "DISTINCT",
          "HAVING",
          "INTERVAL",
          "LIMIT",
          "AS",
          "ALL");

  private static final Set<String> BINARY_OPERATORS =
      Set.of("=", "<>", "!=", "<=", ">=", "<", ">", "+", "-", "*", "/", "%", "||", "=>", ":=");

  enum Kind {
    SPACE,
    NEWLINE,
    LINE_COMMENT,
    BLOCK_COMMENT,
    STRING,
    QUOTED_NAME,
    TEMPLATE,
    WORD,
    NUMBER,
    PARAMETER,
    OPERATOR,
    PUNCTUATION
  }

  record Token(Kind kind, String text) {

    boolean is(String symbol) {
      return (kind == Kind.OPERATOR || kind == Kind.PUNCTUATION) && text.equals(symbol);
    }

    boolean isWord(String word) {
      return kind == Kind.WORD && text.equalsIgnoreCase(word);
    }

    String upper() {
      return text.toUpperCase(Locale.ROOT);
    }

    boolean isComment() {
      return kind == Kind.LINE_COMMENT || kind == Kind.BLOCK_COMMENT;
    }

    /** Whether it is a section, comment or partial of a template, which is not part of a query */
    boolean isTemplateTag() {
      return kind == Kind.TEMPLATE && text.length() > 2 && "#/^!>".indexOf(text.charAt(2)) >= 0;
    }
  }

  private enum Scope {
    STATEMENT,
    PARENTHESES,
    TYPE,
    CASE
  }

  private static final class Context {
    final Scope scope;
    final int base;
    boolean inClause;
    boolean onConditions;
    boolean inSelect;
    boolean itemEnded = true;

    Context(Scope scope, int base) {
      this.scope = scope;
      this.base = base;
    }
  }

  private final String indentUnit;
  private final Deque<Context> contexts = new ArrayDeque<>();
  private boolean statementStarted;
  private boolean distinctStatement;
  private int typeArguments;
  private Token lineEnd;

  private ScriptFormatter(String indentUnit) {
    this.indentUnit = indentUnit;
  }

  /** The edit that formats a script, if it is not formatted yet. */
  static List<TextEdit> edits(String text, FormattingOptions options) {
    var unit =
        options == null || options.isInsertSpaces()
            ? " ".repeat(options == null || options.getTabSize() <= 0 ? 4 : options.getTabSize())
            : "\t";
    return format(text, unit)
        .filter(formatted -> !formatted.equals(text))
        .map(formatted -> List.of(new TextEdit(wholeText(text), formatted)))
        .orElse(List.of());
  }

  /** The formatted script, or none if formatting would change more than whitespace. */
  public static Optional<String> format(String text, String indentUnit) {
    var tokens = tokenize(text);
    var formatted = new ScriptFormatter(indentUnit).layout(tokens, lineSeparator(text));
    return significant(tokens).equals(significant(tokenize(formatted)))
        ? Optional.of(formatted)
        : Optional.empty();
  }

  private static Range wholeText(String text) {
    int line = 0;
    int lineStart = 0;
    for (int i = 0; i < text.length(); i++) {
      if (text.charAt(i) == '\n') {
        line++;
        lineStart = i + 1;
      }
    }
    return new Range(new Position(0, 0), new Position(line, text.length() - lineStart));
  }

  private static String lineSeparator(String text) {
    return text.contains("\r\n") ? "\r\n" : "\n";
  }

  private static List<String> significant(List<Token> tokens) {
    return tokens.stream()
        .filter(token -> token.kind() != Kind.SPACE && token.kind() != Kind.NEWLINE)
        .map(Token::text)
        .toList();
  }

  private String layout(List<Token> tokens, String separator) {
    List<List<Token>> lines = new ArrayList<>();
    var line = new ArrayList<Token>();
    for (var token : tokens) {
      if (token.kind() == Kind.NEWLINE) {
        lines.add(line);
        line = new ArrayList<>();
      } else {
        line.add(token);
      }
    }
    lines.add(line);
    lines = joinTables(lines);

    contexts.push(new Context(Scope.STATEMENT, 0));
    var out = new ArrayList<String>();
    var pendingComments = new ArrayList<Integer>();
    for (var current : lines) {
      var code = code(current);
      if (code.isEmpty()) {
        out.add("");
      } else if (code.stream().allMatch(Token::isComment)) {
        // Comments are indented like the code that follows them
        pendingComments.add(out.size());
        out.add(join(current));
      } else {
        // Templates, e.g. sections, keep their indentation
        var template = code.get(0).kind() == Kind.TEMPLATE;
        var level = template ? leadingSpace(current).length() / indentUnit.length() : indent(code);
        var indent = template ? leadingSpace(current) : indentUnit.repeat(level);
        for (var index : pendingComments) {
          out.set(index, indent + out.get(index));
        }
        pendingComments.clear();
        out.add(indent + join(current));
        track(code, level);
        code.stream()
            .filter(token -> !token.isComment())
            .reduce((a, b) -> b)
            .ifPresent(token -> lineEnd = token);
      }
    }
    var last = indentUnit.repeat(indent(List.of()));
    pendingComments.forEach(index -> out.set(index, last + out.get(index)));

    while (!out.isEmpty() && out.get(0).isEmpty()) {
      out.remove(0);
    }
    while (!out.isEmpty() && out.get(out.size() - 1).isEmpty()) {
      out.remove(out.size() - 1);
    }
    return out.isEmpty() ? "" : String.join(separator, out) + separator;
  }

  /** Moves the table and alias of a join onto its line, e.g. {@code LEFT JOIN Orders o}. */
  private static List<List<Token>> joinTables(List<List<Token>> lines) {
    var joined = new ArrayList<List<Token>>();
    for (var line : lines) {
      var previous = joined.isEmpty() ? null : joined.get(joined.size() - 1);
      var code = code(line);
      if (previous != null && awaitsTable(code(previous)) && isTableName(code)) {
        previous.add(new Token(Kind.SPACE, " "));
        previous.addAll(line.subList(leadingSpace(line).isEmpty() ? 0 : 1, line.size()));
      } else {
        joined.add(new ArrayList<>(line));
      }
    }
    return joined;
  }

  private static List<Token> code(List<Token> line) {
    return line.stream().filter(token -> token.kind() != Kind.SPACE).toList();
  }

  private static boolean awaitsTable(List<Token> code) {
    for (int i = code.size() - 1; i >= 0; i--) {
      if (code.get(i).isWord("JOIN")) {
        return code.subList(i + 1, code.size()).stream().allMatch(ScriptFormatter::isNamePart);
      }
    }
    return false;
  }

  private static boolean isTableName(List<Token> code) {
    return !code.isEmpty() && code.stream().allMatch(ScriptFormatter::isNamePart);
  }

  private static boolean isNamePart(Token token) {
    return switch (token.kind()) {
      case QUOTED_NAME -> true;
      case PUNCTUATION, OPERATOR -> token.is(".");
      case WORD -> {
        var word = token.upper();
        yield !CLAUSES.contains(word)
            && !JOIN_SIDES.contains(word)
            && !word.equals("ON")
            && !word.equals("USING")
            && !word.equals("OUTER");
      }
      default -> false;
    };
  }

  private static String leadingSpace(List<Token> line) {
    return !line.isEmpty() && line.get(0).kind() == Kind.SPACE ? line.get(0).text() : "";
  }

  /** The indentation level of a line from the contexts that are open at its start. */
  private int indent(List<Token> code) {
    var open = new ArrayList<>(contexts);
    var top = 0;
    var closed = -1;
    for (var token : code) {
      var scope = closedScope(token);
      if (scope == null) {
        break;
      }
      var parentheses = top;
      while (parentheses < open.size() && open.get(parentheses).scope != scope) {
        parentheses++;
      }
      if (parentheses == open.size()) {
        break;
      }
      closed = open.get(parentheses).base;
      top = parentheses + 1;
    }
    if (closed >= 0) {
      return closed;
    }
    var context = open.get(0);
    var first = code.isEmpty() ? null : code.get(0);
    var clause = first != null && startsClause(code);
    if (first != null
        && first.isWord("ON")
        && distinctStatement
        && context.scope == Scope.STATEMENT) {
      return 0; // DISTINCT Table ON columns
    }
    // Conditions after a line that starts with ON, and the rest of a selected column, are indented
    var nested =
        first != null
                && !clause
                && ((context.onConditions && !first.isWord("ON"))
                    || (context.inSelect && !context.itemEnded))
            ? 1
            : 0;
    return switch (context.scope) {
      case STATEMENT ->
          !statementStarted || first == null || clause || first.is(":=") ? 0 : 1 + nested;
      case PARENTHESES -> context.base + 1 + (context.inClause && !clause ? 1 + nested : 0);
      case TYPE -> context.base + 1;
      case CASE -> {
        if (first != null && first.isWord("END")) {
          yield context.base;
        }
        yield context.base
            + (first != null && (first.isWord("WHEN") || first.isWord("ELSE")) ? 1 : 2);
      }
    };
  }

  private static Scope closedScope(Token token) {
    return token.is(")") ? Scope.PARENTHESES : token.is(">") ? Scope.TYPE : null;
  }

  private static boolean startsClause(List<Token> code) {
    var first = code.get(0);
    if (first.kind() != Kind.WORD) {
      return false;
    }
    var word = first.upper();
    if (JOIN_SIDES.contains(word)) {
      return code.size() > 1
          && (code.get(1).isWord("JOIN")
              || code.get(1).isWord("OUTER")
              || code.get(1).isWord("ANTI")
              || code.get(1).isWord("SEMI"));
    }
    return CLAUSES.contains(word);
  }

  /** Opens and closes the contexts of a line's parentheses, CASE expressions and statements. */
  private void track(List<Token> code, int level) {
    var lineStart = true;
    var conditions = false;
    for (int i = 0; i < code.size(); i++) {
      var token = code.get(i);
      if (token.isComment() || token.isTemplateTag()) {
        continue;
      }
      statementStarted = true;
      if (lineStart && closedScope(token) == null) {
        lineStart = false;
        var context = contexts.peek();
        if (startsClause(code.subList(i, code.size()))) {
          context.inClause = true;
          context.onConditions = false;
          context.inSelect = token.isWord("SELECT");
          context.itemEnded = true;
          conditions = CONDITIONS.contains(token.upper());
        } else if (token.isWord("ON") && !(distinctStatement && context.scope == Scope.STATEMENT)) {
          context.onConditions = true;
        } else if (token.isWord("THEN") || token.isWord("ELSE")) {
          conditions = true;
        }
      }
      if (token.is("(")) {
        // Parentheses in conditions and results are indented like them
        contexts.push(new Context(Scope.PARENTHESES, level + (conditions ? 1 : 0)));
      } else if (token.is("<") && i > 0 && isTypeWithArguments(code.get(i - 1))) {
        contexts.push(new Context(Scope.TYPE, level));
      } else if (token.is(">") && contexts.peek().scope == Scope.TYPE) {
        contexts.pop();
      } else if (token.is(")")) {
        while (contexts.size() > 1 && contexts.peek().scope != Scope.PARENTHESES) {
          contexts.pop();
        }
        if (contexts.size() > 1) {
          contexts.pop();
        }
      } else if (token.isWord("CASE")) {
        contexts.push(new Context(Scope.CASE, level));
      } else if (token.isWord("END") && contexts.peek().scope == Scope.CASE) {
        contexts.pop();
      } else if (token.is(";")) {
        contexts.clear();
        contexts.push(new Context(Scope.STATEMENT, 0));
        statementStarted = false;
        distinctStatement = false;
      } else if (token.isWord("SELECT") || token.isWord("VALUES")) {
        contexts.peek().inClause = true;
        contexts.peek().inSelect = token.isWord("SELECT");
        contexts.peek().itemEnded = true;
      } else if (token.isWord("DISTINCT") && i > 0 && code.get(i - 1).is(":=")) {
        distinctStatement = true;
      } else if (CLAUSES.contains(token.upper()) && !token.isWord("SELECT")) {
        contexts.peek().inSelect = false;
      }
    }
    // A selected column continues on the next line unless its line ends with a comma
    var last = code.get(code.size() - 1);
    var context = contexts.peek();
    if (context.inSelect && !last.isComment()) {
      context.itemEnded = last.is(",") || last.isWord("SELECT") || last.isWord("DISTINCT");
    }
  }

  /** The tokens of a line with normalized spaces between them, without trailing spaces. */
  private String join(List<Token> line) {
    var text = new StringBuilder();
    Token previous = null;
    Token beforePrevious = null;
    String space = null;
    var statement = line.stream().filter(token -> token.kind() != Kind.SPACE).findFirst();
    var verbatim = statement.map(token -> token.isWord("IMPORT")).orElse(false);
    for (int i = 0; i < line.size(); i++) {
      var token = line.get(i);
      if (token.kind() == Kind.SPACE) {
        space = token.text();
        continue;
      }
      if (previous != null) {
        text.append(
            verbatim || touchesTemplate(line, i)
                ? (space == null ? "" : space)
                : space(beforePrevious, previous, token, space));
      }
      updateTypeArguments(previous, token);
      text.append(token.text());
      beforePrevious = previous == null ? lineEnd : previous;
      previous = token;
      space = null;
    }
    return text.toString();
  }

  /** Whether a token or its neighbour touches a template, whose spacing is kept as written. */
  private static boolean touchesTemplate(List<Token> line, int index) {
    for (int i = Math.max(0, index - 2); i <= Math.min(line.size() - 1, index + 1); i++) {
      if (line.get(i).kind() == Kind.TEMPLATE) {
        return true;
      }
    }
    return false;
  }

  private void updateTypeArguments(Token previous, Token token) {
    if (token.is("<") && previous != null && isTypeWithArguments(previous)) {
      typeArguments++;
    } else if (token.is(">") && typeArguments > 0) {
      typeArguments--;
    }
  }

  private static boolean isTypeWithArguments(Token token) {
    return token.kind() == Kind.WORD && TYPES_WITH_ARGUMENTS.contains(token.upper());
  }

  /** The space between two tokens of a line, given the space between them in the script. */
  private String space(Token beforePrevious, Token previous, Token token, String written) {
    if (token.isComment() || previous.isComment()) {
      return written == null ? "" : written;
    }
    if (token.is(",") || token.is(";") || token.is(")") || token.is("]") || token.is(".")) {
      return "";
    }
    if (previous.is("(") || previous.is("[") || previous.is(".")) {
      return "";
    }
    if (previous.is(",")) {
      return " ";
    }
    if (token.is("<") && isTypeWithArguments(previous)) {
      return "";
    }
    if (typeArguments > 0 && (previous.is("<") || token.is(">"))) {
      return "";
    }
    if (isOperator(token)) {
      return " ";
    }
    if (isOperator(previous)) {
      var unary = (previous.is("-") || previous.is("+")) && isOperandStart(beforePrevious);
      return unary ? "" : " ";
    }
    return written == null ? "" : " ";
  }

  private boolean isOperator(Token token) {
    if (token.kind() != Kind.OPERATOR) {
      return false;
    }
    if (typeArguments > 0 && (token.is("<") || token.is(">"))) {
      return false;
    }
    return BINARY_OPERATORS.contains(token.text());
  }

  /** Whether an operand starts after a token, so that a sign is unary and `*` is a wildcard. */
  private boolean isOperandStart(Token token) {
    return token == null
        || token.is("(")
        || token.is(",")
        || token.is("[")
        || isOperator(token)
        || (token.kind() == Kind.WORD && OPERAND_STARTS.contains(token.upper()));
  }

  static List<Token> tokenize(String text) {
    var tokens = new ArrayList<Token>();
    int i = 0;
    while (i < text.length()) {
      int start = i;
      char c = text.charAt(i);
      Kind kind;
      if (c == '\n' || (c == '\r' && i + 1 < text.length() && text.charAt(i + 1) == '\n')) {
        i += c == '\r' ? 2 : 1;
        kind = Kind.NEWLINE;
      } else if (c == ' ' || c == '\t' || c == '\r' || c == '\f') {
        while (i < text.length() && " \t\f".indexOf(text.charAt(i)) >= 0
            || (i < text.length() && text.charAt(i) == '\r' && !text.startsWith("\r\n", i))) {
          i++;
        }
        kind = Kind.SPACE;
      } else if (text.startsWith("--", i) || text.startsWith("//", i)) {
        while (i < text.length() && text.charAt(i) != '\n' && !text.startsWith("\r\n", i)) {
          i++;
        }
        kind = Kind.LINE_COMMENT;
      } else if (text.startsWith("/*", i)) {
        int end = text.indexOf("*/", i + 2);
        i = end < 0 ? text.length() : end + 2;
        kind = Kind.BLOCK_COMMENT;
      } else if (text.startsWith("{{", i)) {
        var close = text.startsWith("{{{", i) ? "}}}" : "}}";
        int end = text.indexOf(close, i + 2);
        i = end < 0 ? text.length() : end + close.length();
        kind = Kind.TEMPLATE;
      } else if (c == '\'' || c == '"' || c == '`') {
        i = quoted(text, i, c);
        kind = c == '`' ? Kind.QUOTED_NAME : Kind.STRING;
      } else if (Character.isLetter(c) || c == '_' || c == '$') {
        i = word(text, i);
        kind = Kind.WORD;
      } else if (Character.isDigit(c)
          || (c == '.' && i + 1 < text.length() && Character.isDigit(text.charAt(i + 1)))) {
        i = number(text, i);
        kind = Kind.NUMBER;
      } else if (c == ':' && i + 1 < text.length() && isWordStart(text.charAt(i + 1))) {
        i = word(text, i + 1);
        kind = Kind.PARAMETER;
      } else {
        var operator = operator(text, i);
        i += operator.length();
        kind = "(),;.[]{}".contains(operator) ? Kind.PUNCTUATION : Kind.OPERATOR;
      }
      tokens.add(new Token(kind, text.substring(start, i)));
    }
    return tokens;
  }

  private static boolean isWordStart(char c) {
    return Character.isLetter(c) || c == '_';
  }

  private static int quoted(String text, int start, char quote) {
    int i = start + 1;
    while (i < text.length()) {
      if (text.charAt(i) == quote) {
        if (i + 1 < text.length() && text.charAt(i + 1) == quote) {
          i += 2;
          continue;
        }
        return i + 1;
      }
      i++;
    }
    return text.length();
  }

  /** A name, including the hyphens of import paths, e.g. data-sources, which are not operators. */
  private static int word(String text, int start) {
    int i = start;
    while (true) {
      while (i < text.length()
          && (Character.isLetterOrDigit(text.charAt(i))
              || text.charAt(i) == '_'
              || text.charAt(i) == '$')) {
        i++;
      }
      if (i + 1 < text.length()
          && text.charAt(i) == '-'
          && (Character.isLetterOrDigit(text.charAt(i + 1)) || text.charAt(i + 1) == '_')) {
        i++;
        continue;
      }
      return i;
    }
  }

  private static int number(String text, int start) {
    int i = start;
    while (i < text.length()
        && (Character.isLetterOrDigit(text.charAt(i)) || text.charAt(i) == '.')) {
      i++;
    }
    return i;
  }

  private static String operator(String text, int start) {
    for (var operator : List.of(":=", "=>", "<=", ">=", "<>", "!=", "||", "::")) {
      if (text.startsWith(operator, start)) {
        return operator;
      }
    }
    return text.substring(start, start + 1);
  }
}
