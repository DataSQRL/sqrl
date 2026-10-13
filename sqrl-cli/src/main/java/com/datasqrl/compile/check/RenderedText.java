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
package com.datasqrl.compile.check;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Maps offsets in a script that was rendered from a Mustache template, e.g. with {@code
 * {{variant}}} filled in, to offsets in the script as it was written. The text between the tags of
 * the template is the same in both, and an offset in a filled-in value maps to the start of its
 * tag.
 *
 * <p>The rendered script is matched against the template: a variable matches any text, a section
 * ({@code {{#flag}}...{{/flag}}}) is left out or repeated, and an inverted section ({@code
 * {{^flag}}...{{/flag}}}) is left out or rendered once. Mustache leaves out the line of a tag that
 * is alone on its line for some tags, so such lines may or may not be rendered. The text of a
 * section that is repeated maps to the section in the template each time.
 */
final class RenderedText {

  private static final Pattern TAG = Pattern.compile("\\{\\{\\{.*?}}}|\\{\\{.*?}}", Pattern.DOTALL);

  /** Templates that would take longer to match are not mapped */
  private static final int MAX_STEPS = 1_000_000;

  private static final RenderedText SAME = new RenderedText(List.of());

  /**
   * A part of the rendered text and where it is in the source.
   *
   * @param rendered where the part starts in the rendered text
   * @param source where it starts in the source
   * @param literal whether it is text of the source, otherwise it is the value of a tag
   */
  private record Part(int rendered, int source, boolean literal) {}

  private enum Type {
    /** Text of the template, which is rendered as it is */
    LITERAL,
    /** Text that may or may not be rendered, e.g. the line break after a standalone tag */
    OPTIONAL,
    /** A variable or partial, which is rendered as any text */
    VALUE,
    /** The start of a section */
    OPEN,
    /** The end of a section */
    CLOSE,
    /** A comment, which renders nothing */
    COMMENT
  }

  /**
   * An element of the template.
   *
   * @param type what it is
   * @param source where it starts in the source
   * @param text the text of a literal
   * @param other for the start of a section the index of its end, and the other way around
   * @param inverted whether a section is inverted, i.e. rendered at most once
   */
  private record Token(Type type, int source, String text, int other, boolean inverted) {}

  private static final class GiveUp extends RuntimeException {
    GiveUp() {
      super(null, null, false, false);
    }
  }

  private final List<Part> parts;

  private RenderedText(List<Part> parts) {
    this.parts = parts;
  }

  /** The mapping of a rendered script to its source, which maps nothing if they do not match. */
  static RenderedText of(String source, String rendered) {
    if (source.equals(rendered)) {
      return SAME;
    }
    var tokens = tokenize(source);
    if (tokens == null) {
      return SAME;
    }
    var matcher = new Matcher(tokens, rendered);
    try {
      return matcher.match(0, 0) ? new RenderedText(List.copyOf(matcher.path)) : SAME;
    } catch (GiveUp e) {
      return SAME;
    }
  }

  /** The offset in the source of an offset in the rendered text. */
  int toSource(int offset) {
    if (parts.isEmpty()) {
      return offset;
    }
    // The last part that starts at the offset or before, which is the text after an empty value
    var part = parts.get(0);
    for (var next : parts) {
      if (next.rendered() > offset) {
        break;
      }
      part = next;
    }
    return part.literal() ? part.source() + (offset - part.rendered()) : part.source();
  }

  /** Splits a template into its elements, or returns null if it cannot be mapped. */
  private static List<Token> tokenize(String source) {
    var tokens = new ArrayList<Token>();
    var sections = new ArrayDeque<Integer>();
    var tags = TAG.matcher(source);
    var literalStart = 0;
    while (tags.find()) {
      var tag = tags.group();
      var kind = tag.startsWith("{{{") ? '&' : tag.length() > 4 ? tag.charAt(2) : ' ';
      if (kind == '=') {
        return null; // other delimiters
      }
      var type =
          switch (kind) {
            case '#', '^' -> Type.OPEN;
            case '/' -> Type.CLOSE;
            case '!' -> Type.COMMENT;
            default -> Type.VALUE;
          };
      // A tag that renders nothing and is alone on its line may take the line with it
      var lineStart = source.lastIndexOf('\n', tags.start() - 1) + 1;
      var lineEnd = source.indexOf('\n', tags.end());
      var standalone =
          type != Type.VALUE
              && source.substring(lineStart, tags.start()).isBlank()
              && source.substring(tags.end(), lineEnd < 0 ? source.length() : lineEnd).isBlank();
      var before = standalone ? Math.max(lineStart, literalStart) : tags.start();
      addLiteral(tokens, source, literalStart, before, Type.LITERAL);
      addLiteral(tokens, source, before, tags.start(), Type.OPTIONAL);
      if (type == Type.OPEN) {
        sections.push(tokens.size());
        tokens.add(new Token(Type.OPEN, tags.start(), null, -1, kind == '^'));
      } else if (type == Type.CLOSE) {
        if (sections.isEmpty()) {
          return null;
        }
        var open = sections.pop();
        var inverted = tokens.get(open).inverted();
        tokens.set(
            open, new Token(Type.OPEN, tokens.get(open).source(), null, tokens.size(), inverted));
        tokens.add(new Token(Type.CLOSE, tags.start(), null, open, inverted));
      } else {
        tokens.add(new Token(type, tags.start(), null, -1, false));
      }
      literalStart = tags.end();
      if (standalone) {
        var after = lineEnd < 0 ? source.length() : lineEnd + 1;
        addLiteral(tokens, source, tags.end(), after, Type.OPTIONAL);
        literalStart = after;
      }
    }
    if (!sections.isEmpty()) {
      return null;
    }
    addLiteral(tokens, source, literalStart, source.length(), Type.LITERAL);
    return tokens;
  }

  private static void addLiteral(List<Token> tokens, String source, int start, int end, Type type) {
    if (end > start) {
      tokens.add(new Token(type, start, source.substring(start, end), -1, false));
    }
  }

  /** Matches the rendered text against the elements of the template, from the left. */
  private static final class Matcher {

    private final List<Token> tokens;
    private final String rendered;
    private final List<Part> path = new ArrayList<>();
    private final Set<Long> failed = new HashSet<>();
    private final Set<Long> active = new HashSet<>();
    private int steps = 0;

    Matcher(List<Token> tokens, String rendered) {
      this.tokens = tokens;
      this.rendered = rendered;
    }

    /** Whether the rendered text from a position matches the template from an element. */
    boolean match(int index, int position) {
      if (++steps > MAX_STEPS) {
        throw new GiveUp();
      }
      if (index == tokens.size()) {
        return position == rendered.length();
      }
      var key = ((long) index << 32) | position;
      // A section that is repeated without rendering anything is not repeated again
      if (failed.contains(key) || !active.add(key)) {
        return false;
      }
      try {
        var matched = matchToken(index, position);
        if (!matched) {
          failed.add(key);
        }
        return matched;
      } finally {
        active.remove(key);
      }
    }

    private boolean matchToken(int index, int position) {
      var token = tokens.get(index);
      return switch (token.type()) {
        case LITERAL, OPTIONAL -> {
          if (rendered.startsWith(token.text(), position)
              && matchPart(index + 1, position + token.text().length(), position, token, true)) {
            yield true;
          }
          yield token.type() == Type.OPTIONAL && match(index + 1, position);
        }
        case VALUE -> matchValue(index, position, token);
        // Rendered, or left out
        case OPEN -> match(index + 1, position) || match(token.other() + 1, position);
        // Done, or repeated
        case CLOSE ->
            match(index + 1, position) || (!token.inverted() && match(token.other() + 1, position));
        case COMMENT -> match(index + 1, position);
      };
    }

    /** Matches a value, trying the shortest values that the next literal can follow first. */
    private boolean matchValue(int index, int position, Token token) {
      var next = index + 1 < tokens.size() ? tokens.get(index + 1) : null;
      if (next != null && next.type() == Type.LITERAL) {
        for (var end = rendered.indexOf(next.text(), position);
            end >= 0;
            end = rendered.indexOf(next.text(), end + 1)) {
          if (matchPart(index + 1, end, position, token, false)) {
            return true;
          }
        }
        return false;
      }
      for (var end = position; end <= rendered.length(); end++) {
        if (matchPart(index + 1, end, position, token, false)) {
          return true;
        }
      }
      return false;
    }

    /** Matches the rest after a part of the rendered text, which is kept if the rest matches. */
    private boolean matchPart(int next, int end, int start, Token token, boolean literal) {
      path.add(new Part(start, token.source(), literal));
      if (match(next, end)) {
        return true;
      }
      path.remove(path.size() - 1);
      return false;
    }
  }
}
