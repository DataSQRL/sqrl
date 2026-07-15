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

import static com.datasqrl.planner.parser.SqrlStatementParser.relativeLocation;
import static com.datasqrl.planner.parser.StatementParserException.checkFatal;
import static com.google.common.base.Preconditions.checkArgument;

import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.google.common.base.Supplier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Represents a hint in SQRL. SQRL hints give the user control over many aspects of the planning
 * process.
 */
public record SqrlHint(String name, List<String> options) {

  private static final Pattern HINT_PATTERN =
      Pattern.compile(
          "\\s*(?<name>\\w+)(?:\\((?<args>[^)]*)\\))?\\s*(,\\s*|$)",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private static final Pattern HINT_ARGS_PATTERN = Pattern.compile("[\\w`,\\s]*");

  public static List<ParsedObject<SqrlHint>> parse(ParsedObject<String> hint) {
    checkArgument(hint.isPresent());

    var hintMatcher = HINT_PATTERN.matcher(hint.get());
    var hints = new ArrayList<ParsedObject<SqrlHint>>();
    var lastMatchEnd = 0;
    while (hintMatcher.find()) {
      checkFatal(
          lastMatchEnd == hintMatcher.start(),
          relativeLocation(hint, lastMatchEnd),
          ErrorCode.INVALID_HINT,
          "Hint block contains non-hints");

      var hintName = hintMatcher.group("name");
      var rawArgs = hintMatcher.group("args");
      var arguments = parseArgs(rawArgs, () -> relativeLocation(hint, hintMatcher.start("args")));
      var loc =
          hint.getFileLocation()
              .add(SqrlStatementParser.computeFileLocation(hint.get(), hintMatcher.start()));

      lastMatchEnd = hintMatcher.end();
      hints.add(new ParsedObject<>(new SqrlHint(hintName, arguments), loc));
    }

    checkFatal(
        lastMatchEnd == hint.get().length(),
        relativeLocation(hint, lastMatchEnd),
        ErrorCode.INVALID_HINT,
        "Hint block contains non-hints");

    return hints;
  }

  private static List<String> parseArgs(String args, Supplier<FileLocation> locationSupplier) {
    if (args == null) {
      return List.of();
    }

    checkFatal(
        HINT_ARGS_PATTERN.matcher(args).matches(),
        locationSupplier.get(),
        ErrorCode.INVALID_HINT_ARG,
        "Hint contains invalid argument characters");

    return Arrays.stream(args.split(",")).map(String::trim).toList();
  }
}
