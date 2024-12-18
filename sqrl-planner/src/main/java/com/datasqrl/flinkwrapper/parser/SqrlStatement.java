package com.datasqrl.flinkwrapper.parser;

import com.datasqrl.canonicalizer.NamePath;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.sql.SqlIdentifier;

@Value
public class SqrlStatement {

  public static final String DISTINCT_REGEX = "DISTINCT\\s+(?<from>"+SqrlStatementParser.IDENTIFIER_REGEX+")\\s+ON\\s+(?<columns>.+?)\\s+ORDER\\s+BY\\s+(?<remaining>.*);";

  public static final Pattern DISTINCT_PARSER = Pattern.compile(DISTINCT_REGEX,
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  Type type;
  Optional<SqlIdentifier> identifier;
  Optional<String> identifierString;
  Optional<SqlIdentifier> packageIdentifier;
  Optional<String> definition;
  String comments; //for hints before the statement

  public boolean isDefinition() {
    return type.isDefinition;
  }

  public String getCommentString() {
    if (Strings.isNullOrEmpty(comments)) return "";
    else return comments;
  }

  public String rewriteSQL() {
    Preconditions.checkArgument(isDefinition());
    String body = definition.get();
    switch (type) {
      case TABLE:
        return String.format("%sCREATE TEMPORARY VIEW %s AS %s", getCommentString(), identifierString.get(),body);
      case RELATIONSHIP:
        return String.format("%sSELECT * FROM %s this %s", getCommentString(), identifierString.get(),body);
      case DISTINCT:
        //TODO: A lightweight parser would do better here
        Matcher matcher = DISTINCT_PARSER.matcher(definition.get());
        Preconditions.checkArgument(matcher.find(), "Could not parse [DISTINCT] statement due to invalid syntax: %s", definition.get());
        String from = matcher.group("from");
        String columns = matcher.group("columns");
        String remaining = matcher.group("remaining");
        return String.format("%sCREATE TEMPORARY VIEW %s AS SELECT %s FROM (SELECT * FROM %s ORDER BY %s);",
            getCommentString(), identifierString.get(), columns, from, remaining);
      default: throw new IllegalArgumentException(type.toString());
    }
  }


  @AllArgsConstructor
  @Getter
  public enum Type {
    IMPORT("import", false),
    EXPORT("export", false),
    DISTINCT("distinct", true),
    RELATIONSHIP("join", true),
    TABLE("select", true);

    private final String keyword;
    private final boolean isDefinition;

    @Override
    public String toString() {
      return keyword.toUpperCase();
    }

    public static Optional<Type> fromStatement(String body) {
      for (Type type : Type.values()) {
        if (body.trim().toLowerCase().startsWith(type.keyword)) {
          return Optional.of(type);
        }
      }
      return Optional.empty();
    }

    public static Collection<Type> getDefinitions() {
      return Arrays.stream(values()).filter(Type::isDefinition).collect(Collectors.toUnmodifiableList());
    }
  }

}
