package com.datasqrl.planner.parser;

import static com.datasqrl.planner.parser.SqrlStatementParser.relativeLocation;
import static com.datasqrl.planner.parser.StatementParserException.checkFatal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.datasqrl.error.ErrorCode;
import com.google.common.base.Preconditions;

import lombok.Value;

/**
 * Represents a hint in SQRL.
 * SQRL hints give the user control over many aspects of the planning process.
 */

@Value
public class SqrlHint {

  public static final Pattern hintPattern = Pattern.compile("\\s*(?<name>\\w+)(?:\\((?<args>[\\w`,\\s]*)\\))?\\s*(,\\s*|$)",
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  String name;
  List<String> options;

  public static List<ParsedObject<SqrlHint>> parse(ParsedObject<String> hint) {
    Preconditions.checkArgument(hint.isPresent());
    var hintMatcher = hintPattern.matcher(hint.get());
    List<ParsedObject<SqrlHint>> hints = new ArrayList<>();
    var lastMatchEnd = 0;
    while (hintMatcher.find()) {
      checkFatal(lastMatchEnd==hintMatcher.start(), relativeLocation(hint, lastMatchEnd), ErrorCode.INVALID_HINT, "Hint block contains non-hints");
      var hintName = hintMatcher.group("name");
      var argumentStr = hintMatcher.group("args");
      List<String> arguments = List.of();
      if (argumentStr != null) {
        arguments = Arrays.stream(hintMatcher.group(2).split(",")).map(String::trim).collect(
            Collectors.toUnmodifiableList());
      }
      lastMatchEnd = hintMatcher.end();
      var loc = hint.getFileLocation().add(SqrlStatementParser.computeFileLocation(hint.get(),
          hintMatcher.start()));
      hints.add(new ParsedObject<>(new SqrlHint(hintName,arguments), loc));
    }
    checkFatal(lastMatchEnd==hint.get().length(), relativeLocation(hint, lastMatchEnd), ErrorCode.INVALID_HINT, "Hint block contains non-hints");

    return hints;
  }

}
