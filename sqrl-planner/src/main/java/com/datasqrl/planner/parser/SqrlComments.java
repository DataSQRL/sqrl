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
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;

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

  public SqrlComments removeHintsByName(Predicate<String> remove) {
    return new SqrlComments(
        documentation,
        hints.stream().filter(p -> !p.isPresent() || !remove.test(p.get().getName())).toList());
  }

  public boolean containsHintByName(Predicate<String> match) {
    return hints.stream()
        .filter(ParsedObject::isPresent)
        .map(ParsedObject::get)
        .map(SqrlHint::getName)
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
}
