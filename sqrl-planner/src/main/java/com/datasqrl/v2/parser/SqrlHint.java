package com.datasqrl.v2.parser;

import static com.datasqrl.v2.parser.SqrlStatementParser.relativeLocation;
import static com.datasqrl.v2.parser.StatementParserException.checkFatal;

import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.Value;

@Value
public class SqrlHint {

  public static final Pattern hintPattern = Pattern.compile("\\s*(?<name>\\w+)(?:\\((?<args>[\\w`,\\s]*)\\))?\\s*(,\\s*|$)",
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  String name;
  List<String> options;

  public static List<ParsedObject<SqrlHint>> parse(ParsedObject<String> hint) {
    Preconditions.checkArgument(hint.isPresent());
    Matcher hintMatcher = hintPattern.matcher(hint.get());
    List<ParsedObject<SqrlHint>> hints = new ArrayList<>();
    int lastMatchEnd = 0;
    while (hintMatcher.find()) {
      checkFatal(lastMatchEnd==hintMatcher.start(), relativeLocation(hint, lastMatchEnd), ErrorCode.INVALID_HINT, "Hint block contains non-hints");
      String hintName = hintMatcher.group("name");
      String argumentStr = hintMatcher.group("args");
      List<String> arguments = List.of();
      if (argumentStr != null) {
        arguments = Arrays.stream(hintMatcher.group(2).split(",")).map(String::trim).collect(
            Collectors.toUnmodifiableList());
      }
      lastMatchEnd = hintMatcher.end();
      FileLocation loc = hint.getFileLocation().add(SqrlStatementParser.computeFileLocation(hint.get(),
          hintMatcher.start()));
      hints.add(new ParsedObject<>(new SqrlHint(hintName,arguments), loc));
    }
    checkFatal(lastMatchEnd==hint.get().length(), relativeLocation(hint, lastMatchEnd), ErrorCode.INVALID_HINT, "Hint block contains non-hints");

    return hints;
  }

}
