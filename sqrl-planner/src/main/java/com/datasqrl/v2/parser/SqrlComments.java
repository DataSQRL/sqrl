package com.datasqrl.v2.parser;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.Value;

@Value
public class SqrlComments {

  public static final SqrlComments EMPTY = new SqrlComments(List.of(), List.of());

  List<ParsedObject<String>> documentation;
  List<ParsedObject<SqrlHint>> hints;

  public static final Pattern COMMENT_PATTERN = Pattern.compile(SqrlStatementParser.COMMENT_REGEX);
  public static final String HINT_PREFIX = "/*+";
  public static final String DOC_PREFIX = "/**";

  public SqrlComments removeHintsByName(Predicate<String> remove) {
    return new SqrlComments(documentation, hints.stream().filter(p -> !p.isPresent()
        || !remove.test(p.get().getName())).collect(Collectors.toUnmodifiableList()));
  }

  public boolean containsHintByName(Predicate<String> match) {
    return hints.stream().filter(ParsedObject::isPresent)
        .map(ParsedObject::get).map(SqrlHint::getName).anyMatch(match);
  }

  public static SqrlComments parse(ParsedObject<String> comments) {
    if (comments.isEmpty() || Strings.isNullOrEmpty(comments.get())) return new SqrlComments(List.of(),List.of());
    Matcher commentMatcher = COMMENT_PATTERN.matcher(comments.get());
    List<ParsedObject<String>> docComments = new ArrayList<>();
    List<ParsedObject<SqrlHint>> hintComments = new ArrayList<>();
    while (commentMatcher.find()) {
      String comment = commentMatcher.group();
      String commentContent = comment.substring(3, Math.max(comment.length()-2,3));
      if (Strings.isNullOrEmpty(commentContent)) continue;
      ParsedObject<String> parsedComment = comments.fromOffset(SqrlStatementParser.parse(commentContent,
          comments.get(), commentMatcher.start()+3));
      if (comment.startsWith(HINT_PREFIX)) {
        //Parse hints
        hintComments.addAll(SqrlHint.parse(parsedComment));
      } else if (comment.startsWith(DOC_PREFIX)) {
        docComments.add(parsedComment);
      }
    }
    return new SqrlComments(docComments, hintComments);
  }

}
