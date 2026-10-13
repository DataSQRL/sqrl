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

import com.datasqrl.compile.check.ScriptText;
import com.datasqrl.compile.check.ScriptText.NamePart;
import com.datasqrl.compile.check.SymbolIndex;
import com.datasqrl.compile.check.SymbolIndex.Kind;
import com.datasqrl.compile.check.SymbolIndex.SourceRange;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import org.eclipse.lsp4j.SemanticTokenModifiers;
import org.eclipse.lsp4j.SemanticTokenTypes;
import org.eclipse.lsp4j.SemanticTokens;
import org.eclipse.lsp4j.SemanticTokensLegend;

/**
 * The semantic tokens of a script: the names of tables, functions, relationships, columns and
 * databases that the last checks resolved, so that editors can color them by what they are, which
 * the grammar of the language cannot tell. Definitions have the {@code declaration} modifier.
 *
 * <p>While a script is edited, the tokens of the lines that did not change since the check are
 * kept.
 */
class SemanticHighlighting {

  private static final List<String> TYPES =
      List.of(
          SemanticTokenTypes.Class,
          SemanticTokenTypes.Function,
          SemanticTokenTypes.Method,
          SemanticTokenTypes.Property,
          SemanticTokenTypes.Namespace);
  private static final int TABLE = 0;
  private static final int FUNCTION = 1;
  private static final int RELATIONSHIP = 2;
  private static final int COLUMN = 3;
  private static final int DATABASE = 4;

  private static final List<String> MODIFIERS = List.of(SemanticTokenModifiers.Declaration);
  private static final int DECLARATION = 1;

  static final SemanticTokensLegend LEGEND = new SemanticTokensLegend(TYPES, MODIFIERS);

  /** A token on one line. */
  record Token(int line, int start, int length, int type, int modifiers) {}

  private final Path file;
  private final ScriptText script;
  private final List<Token> tokens = new ArrayList<>();

  /** A template variable whose value is a name, e.g. {@code {{partitionCol}}} */
  private static final Pattern TEMPLATE_VALUE = Pattern.compile("\\{\\{\\{?\\s*([\\w.]+)\\s*}?}}");

  private SemanticHighlighting(Path file, String text) {
    this.file = file;
    this.script = new ScriptText(text);
  }

  static SemanticTokens of(Path file, String text, List<SymbolIndex> indexes) {
    var highlighting = new SemanticHighlighting(file, text);
    indexes.forEach(highlighting::add);
    return new SemanticTokens(encode(highlighting.tokens));
  }

  private void add(SymbolIndex index) {
    var checked = index.texts().get(file);
    if (checked == null) {
      return;
    }
    var lines = ChangedLines.between(checked, script.text());
    var checkedScript = new ScriptText(checked);
    for (var symbol : index.symbols()) {
      addLast(lines, checkedScript, symbol.location(), type(symbol.kind()), DECLARATION);
      symbol
          .columnLocations()
          .values()
          .forEach(range -> addLast(lines, checkedScript, range, COLUMN, DECLARATION));
    }
    for (var reference : index.references()) {
      var range = reference.location();
      var parts = parts(checkedScript, range);
      if (reference.isColumn()) {
        // The column and its nested fields
        parts.forEach(part -> add(lines, range, part, COLUMN, 0));
        continue;
      }
      var type = index.symbol(reference.table()).map(s -> type(s.kind())).orElse(TABLE);
      for (var i = 0; i < parts.size(); i++) {
        var part = parts.get(i);
        if (i == parts.size() - 1) {
          add(lines, range, part, type, 0);
        } else if (part.name().equalsIgnoreCase(reference.table().getDatabaseName())) {
          add(lines, range, part, DATABASE, 0);
        }
      }
    }
    var databases = new HashSet<String>();
    index
        .symbols()
        .forEach(s -> databases.add(s.identifier().getDatabaseName().toLowerCase(Locale.ROOT)));
    for (var link : index.imports()) {
      // The database that a script is imported as, i.e. its name or alias
      var target = link.target();
      if (target.startLine() == 0
          && target.startCharacter() == 0
          && target.endLine() == 0
          && target.endCharacter() == 0) {
        var parts = parts(checkedScript, link.location());
        if (!parts.isEmpty()
            && databases.contains(parts.get(parts.size() - 1).name().toLowerCase(Locale.ROOT))) {
          add(lines, link.location(), parts.get(parts.size() - 1), DATABASE, DECLARATION);
        }
      }
    }
  }

  /** Adds the last part of a name, e.g. {@code orders} of {@code Customers.orders}. */
  private void addLast(
      ChangedLines lines, ScriptText checked, SourceRange range, int type, int modifiers) {
    var parts = parts(checked, range);
    if (!parts.isEmpty()) {
      add(lines, range, parts.get(parts.size() - 1), type, modifiers);
    }
  }

  /** Adds a part of the name at a range, on the line it moved to since the check. */
  private void add(ChangedLines lines, SourceRange range, NamePart part, int type, int modifiers) {
    var line = lines.line(range.startLine());
    if (line.isPresent()) {
      tokens.add(
          new Token(
              line.getAsInt(),
              range.startCharacter() + part.start(),
              part.end() - part.start(),
              type,
              modifiers));
    }
  }

  /** The parts of a name on one line of the checked script, relative to the start of the range. */
  private List<NamePart> parts(ScriptText checked, SourceRange range) {
    if (!range.file().equals(file) || range.startLine() != range.endLine()) {
      return List.of();
    }
    var text = checked.line(range.startLine());
    if (range.endCharacter() > text.length() || range.startCharacter() >= range.endCharacter()) {
      return List.of();
    }
    var name = text.substring(range.startCharacter(), range.endCharacter());
    var template = TEMPLATE_VALUE.matcher(name);
    if (template.matches()) {
      // A name that a template variable stands for, which is colored by its variable
      return List.of(new NamePart(template.group(1), template.start(1), template.end(1)));
    }
    return ScriptText.parseName(name).orElse(List.of());
  }

  private static int type(Kind kind) {
    return switch (kind) {
      case TABLE -> TABLE;
      case FUNCTION -> FUNCTION;
      case RELATIONSHIP -> RELATIONSHIP;
    };
  }

  /**
   * Encodes tokens as the protocol does: each token is five numbers, its line and start relative to
   * the previous token, its length, type and modifiers. Tokens may not overlap, so of overlapping
   * tokens only the first is kept.
   */
  static List<Integer> encode(List<Token> tokens) {
    var sorted =
        tokens.stream()
            .sorted(Comparator.comparingInt(Token::line).thenComparingInt(Token::start))
            .toList();
    var data = new ArrayList<Integer>();
    var line = 0;
    var start = 0;
    var end = 0;
    for (var token : sorted) {
      if (token.line() == line && token.start() < end && !data.isEmpty()) {
        continue;
      }
      data.add(token.line() - line);
      data.add(token.line() == line ? token.start() - start : token.start());
      data.add(token.length());
      data.add(token.type());
      data.add(token.modifiers());
      line = token.line();
      start = token.start();
      end = token.start() + token.length();
    }
    return data;
  }
}
