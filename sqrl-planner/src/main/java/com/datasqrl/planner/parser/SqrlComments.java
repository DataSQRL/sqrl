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

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents the comments on a SQRL statement which are either hints or doc-strings. Other columns
 * are ignored.
 *
 * <p>Note that `--` comments are filtered out by {@link SqlScriptStatementSplitter}
 */
public record SqrlComments(
    List<ParsedObject<String>> documentation, List<ParsedObject<SqrlHint>> hints) {

  public static final SqrlComments EMPTY = new SqrlComments(List.of(), List.of());

  public static final Pattern COMMENT_PATTERN = Pattern.compile(SqrlStatementParser.COMMENT_REGEX);
  public static final String HINT_PREFIX = "/*+";
  public static final String DOC_PREFIX = "/**";

  /**
   * The {@code *} that starts a line of a doc comment, with its indentation and the space after it.
   * It is indented, unlike a {@code *} list bullet that starts the line, and followed by a space or
   * the end of the line, so Markdown bold ({@code **}) is not one.
   */
  private static final Pattern LINE_DECORATION_PATTERN = Pattern.compile("^\\s+\\*(?: |$)");

  /**
   * Returns the text of the doc comments, each trimmed, one after the other on separate lines, or
   * empty if there are none.
   *
   * <p>A doc comment that starts every line after the first with an indented {@code *}, as in a
   * comment that starts each line with {@code " * "}, has that {@code *} and the space after it
   * removed from those lines.
   */
  public Optional<String> docString() {
    if (documentation.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(
        documentation.stream()
            .map(ParsedObject::get)
            .map(SqrlComments::removeLineDecoration)
            .map(String::trim)
            .collect(Collectors.joining("\n")));
  }

  public SqrlComments removeHintsByName(Predicate<String> remove) {
    return new SqrlComments(
        documentation,
        hints.stream().filter(p -> !p.isPresent() || !remove.test(p.get().name())).toList());
  }

  public boolean containsHintByName(Predicate<String> match) {
    return hints.stream()
        .filter(ParsedObject::isPresent)
        .map(ParsedObject::get)
        .map(SqrlHint::name)
        .anyMatch(match);
  }

  public static SqrlComments parse(ParsedObject<String> comments) {
    if (comments.isEmpty() || Strings.isNullOrEmpty(comments.get())) {
      return new SqrlComments(List.of(), List.of());
    }
    var commentMatcher = COMMENT_PATTERN.matcher(comments.get());
    List<ParsedObject<String>> docComments = new ArrayList<>();
    List<ParsedObject<SqrlHint>> hintComments = new ArrayList<>();
    while (commentMatcher.find()) {
      var comment = commentMatcher.group();
      var commentContent = comment.substring(3, Math.max(comment.length() - 2, 3));
      if (Strings.isNullOrEmpty(commentContent)) {
        continue;
      }
      ParsedObject<String> parsedComment =
          comments.fromOffset(
              SqrlStatementParser.parse(
                  commentContent, comments.get(), commentMatcher.start() + 3));
      if (comment.startsWith(HINT_PREFIX)) {
        // Parse hints
        hintComments.addAll(SqrlHint.parse(parsedComment));
      } else if (comment.startsWith(DOC_PREFIX)) {
        docComments.add(parsedComment);
      }
    }
    return new SqrlComments(docComments, hintComments);
  }

  /**
   * Removes the {@code *} that starts each line of a doc comment after the first, if every one of
   * these lines other than blank lines starts with an indented one. The first line is the one that
   * the comment opens on.
   */
  private static String removeLineDecoration(String comment) {
    var lines = comment.split("\\r?\\n", -1);
    var nextLines = Arrays.stream(lines).skip(1).filter(line -> !line.isBlank()).toList();
    var decorated =
        !nextLines.isEmpty()
            && nextLines.stream()
                .allMatch(line -> LINE_DECORATION_PATTERN.matcher(line).lookingAt());

    if (!decorated) {
      return comment;
    }

    return Stream.concat(
            Stream.of(lines[0]),
            Arrays.stream(lines)
                .skip(1)
                .map(line -> LINE_DECORATION_PATTERN.matcher(line).replaceFirst("")))
        .collect(Collectors.joining("\n"));
  }
}
