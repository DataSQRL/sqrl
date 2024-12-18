package com.datasqrl.flinkwrapper.parser;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.flinkwrapper.parser.SqrlStatement.Type;
import com.google.common.base.Preconditions;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.runtime.Pattern.Op;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

public class SqrlStatementParser {

  public static final String IDENTIFIER_REGEX = "[\\w\\.`-]+?";
  public static final String BEGINNING_COMMENT = "^(((/\\*)+?[\\w\\W]*?(\\*/))|(\\s*))*";
  public static final String IMPORT_EXPORT_REGEX = BEGINNING_COMMENT + "(?<fullmatch>import|export)";
  public static final String SQRL_DEFINITION_REGEX = BEGINNING_COMMENT + "(?<fullmatch>(?<tablename>"+IDENTIFIER_REGEX+")\\s*(:=))";

  public static final Pattern IMPORT_PARSER = Pattern.compile("(?<package>"+IDENTIFIER_REGEX+")(\\s+AS\\s+(?<identifier>"+IDENTIFIER_REGEX+")\\s*)?;",
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  public static final Pattern EXPORT_PARSER = Pattern.compile("(?<identifier>"+IDENTIFIER_REGEX+")\\s+TO\\s+(?<package>"+IDENTIFIER_REGEX+")\\s*;",
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private static final Pattern IMPORT_EXPORT = Pattern.compile(IMPORT_EXPORT_REGEX,
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private static final Pattern SQRL_DEFINITION = Pattern.compile(SQRL_DEFINITION_REGEX,
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  public SqrlStatementParser() {

  }

  public Optional<SqrlStatement> parse(String statement) throws SqlParseException {
    int commentEnd = 0;

    Matcher importExportMatcher = IMPORT_EXPORT.matcher(statement);
    if (importExportMatcher.find()) { //it's an import or export statement
      String directiveName = importExportMatcher.group("fullmatch");
      commentEnd = importExportMatcher.end()-directiveName.length();
      String comment = statement.substring(0,commentEnd);
      String body = statement.substring(importExportMatcher.end()).trim();
      SqrlStatement.Type directive = SqrlStatement.Type.fromStatement(directiveName).get();
      Preconditions.checkArgument(directive == Type.IMPORT || directive == Type.EXPORT);

      //TODO: use a lightweight parser for this for better error-handling
      Pattern pattern = directive==Type.IMPORT?IMPORT_PARSER:EXPORT_PARSER;
      Matcher matcher = pattern.matcher(body);
      Preconditions.checkArgument(matcher.find(), "Could not parse [%s] statement due to invalid syntax: %s", directive, body);
      String identifier = matcher.group("identifier");
      String packageIdentifier = matcher.group("package");
      return Optional.of(new SqrlStatement(directive,
          Optional.ofNullable(parseIdentifier(identifier)), Optional.ofNullable(identifier),
          Optional.of(parseIdentifier(packageIdentifier)), Optional.empty(), comment));
    }

    Matcher sqrlDefinition = SQRL_DEFINITION.matcher(statement);
    if (sqrlDefinition.find()) { //it's an import or export statement
      String tableName = sqrlDefinition.group("tablename");
      commentEnd = sqrlDefinition.end()-sqrlDefinition.group("fullmatch").length();
      String comment = statement.substring(0,commentEnd);
      String definition = statement.substring(sqrlDefinition.end()).trim();
      Optional<SqrlStatement.Type> type = SqrlStatement.Type.fromStatement(definition);
      Preconditions.checkArgument(type.isPresent(), "Invalid SQRL definition, expected one of [%s]:\n %s",
          SqrlStatement.Type.getDefinitions(), definition);
      return Optional.of(new SqrlStatement(type.get(),
          Optional.of(parseIdentifier(tableName)), Optional.ofNullable(tableName),
          Optional.empty(), Optional.of(definition), comment));
    }

    //TODO: add support for table functions

    return Optional.empty();
  }

  private SqlIdentifier parseIdentifier(String identifier) throws SqlParseException {
    if (identifier==null) return null;
    SqlParser parser = SqlParser.create(identifier);
    SqlNode sqlNode = parser.parseExpression();

    if (sqlNode instanceof SqlIdentifier) {
      return (SqlIdentifier) sqlNode;
    } else {
      throw new IllegalArgumentException("The provided string is not a valid SQL identifier: " + identifier);
    }
  }

}
