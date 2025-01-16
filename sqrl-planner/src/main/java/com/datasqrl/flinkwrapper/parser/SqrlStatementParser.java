package com.datasqrl.flinkwrapper.parser;

import static com.datasqrl.flinkwrapper.parser.StatementParserException.checkFatal;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.lang3.tuple.Pair;

/**
 * A lightweight REGEX based parser to identify and parse SQRL specific SQL statements.
 *
 * The goal is to pass most of the actual parsing into the Flink parser to remain compatible with Flink and
 * make it easier to maintain that compatibility. That's why we are using a REGEX based approach to wrap around
 * the Calcite parser that Flink uses.
 *
 * A particular challenge is maintaining the AST offsets to correctly pinpoint the source of errors.
 * This is handled through {@link ParsedObject}.
 */
@AllArgsConstructor(onConstructor_=@Inject)
public class SqrlStatementParser {

  public static final String IDENTIFIER_REGEX = "[\\w\\.`*-]+?";
  public static final String COMMENT_REGEX = "(/\\*)+?[\\w\\W]*?(\\*/)";
  public static final String BEGINNING_COMMENT = "^(("+ COMMENT_REGEX +")|(\\s*))*";
  public static final String IMPORT_EXPORT_REGEX = BEGINNING_COMMENT + "(?<fullmatch>import|export)";
  public static final String SQRL_DEFINITION_REGEX = BEGINNING_COMMENT + "(?<fullmatch>(?<tablename>"+IDENTIFIER_REGEX+")\\s*(\\((?<arguments>[\\w$:,()\\s]+?)\\))?\\s*:=)";
  public static final String CREATE_TABLE_REGEX = BEGINNING_COMMENT + "(?<fullmatch>create\\s+(temporary\\s+)?table)";


  public static final Pattern IMPORT_PARSER = Pattern.compile("(?<package>"+IDENTIFIER_REGEX+")(\\s+AS\\s+(?<identifier>"+IDENTIFIER_REGEX+")\\s*)?;",
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  public static final Pattern EXPORT_PARSER = Pattern.compile("(?<identifier>"+IDENTIFIER_REGEX+")\\s+TO\\s+(?<package>"+IDENTIFIER_REGEX+")\\s*;",
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private static final Pattern IMPORT_EXPORT = Pattern.compile(IMPORT_EXPORT_REGEX,
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  private static final Pattern SQRL_DEFINITION = Pattern.compile(SQRL_DEFINITION_REGEX,
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  private static final Pattern CREATE_TABLE = Pattern.compile(CREATE_TABLE_REGEX,
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  public static final String DISTINCT_REGEX = "DISTINCT\\s+(?<from>"+SqrlStatementParser.IDENTIFIER_REGEX+")\\s+ON\\s+(?<columns>.+?)\\s+ORDER\\s+BY\\s+(?<remaining>.*);";

  public static final Pattern DISTINCT_PARSER = Pattern.compile(DISTINCT_REGEX,
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  public static final String SELECT_KEYWORD = "select";
  public static final String JOIN_KEYWORD = "join";
  public static final String DISTINCT_KEYWORD = "distinct";
  public static final String SUBSCRIBE_KEYWORD = "subscribe";


  public static final char ARGUMENT_PREFIX = '$';
  public static final String ARGUMENT_REGEX = "\\s*\\$(?<name>\\w+)\\s*:\\s*(?<type>[\\w()]+?)\\s*(,\\s*|$)";
  public static final Pattern ARGUMENT_PARSER = Pattern.compile(ARGUMENT_REGEX,
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private SqlScriptStatementSplitter sqlSplitter;

  public List<ParsedObject<SQLStatement>> parseScript(String script, ErrorCollector scriptErrors) {
    List<ParsedObject<SQLStatement>> sqlStatements = new ArrayList<>();
    ErrorCollector localErrors = scriptErrors;
    try {
      List<ParsedObject<String>> statements = sqlSplitter.splitStatements(script);
      for (ParsedObject<String> statement : statements) {
        localErrors = scriptErrors.atFile(statement.getFileLocation());
        sqlStatements.add(statement.map(this::parseStatement));
     }
    } catch (StatementParserException e) {
      throw localErrors.handle(e);
    }
    return sqlStatements;
  }

  public SQLStatement parseStatement(String statement) {
    Matcher importExportMatcher = IMPORT_EXPORT.matcher(statement);
    if (importExportMatcher.find()) { //it's an import or export statement
      String directive = importExportMatcher.group("fullmatch");
      int commentEnd = importExportMatcher.end()-directive.length();
      String comment = statement.substring(0,commentEnd); //For now, hints on import/export are ignored
      String body = statement.substring(importExportMatcher.end()).trim();

      //#1: Imports and Exports
      if (directive.equalsIgnoreCase("import")) {
        Matcher subMatcher = IMPORT_PARSER.matcher(body);
        checkFatal(subMatcher.find(), ErrorCode.INVALID_IMPORT, "Could not parse IMPORT statement");
        ParsedObject<NamePath> packageIdentifier = parseNamePath(subMatcher, "package", statement);
        ParsedObject<NamePath> alias = parseNamePath(subMatcher, "identifier", statement);
        checkFatal(packageIdentifier.isPresent(), ErrorCode.INVALID_IMPORT, "Missing package path");
        return new SqrlImportStatement(packageIdentifier, alias);
      } else if (directive.equalsIgnoreCase("export")) {
        Matcher subMatcher = EXPORT_PARSER.matcher(body);
        checkFatal(subMatcher.find(), ErrorCode.INVALID_IMPORT, "Could not parse IMPORT statement");
        ParsedObject<NamePath> packageIdentifier = parseNamePath(subMatcher, "package", statement);
        ParsedObject<NamePath> tableName = parseNamePath(subMatcher, "identifier", statement);
        checkFatal(packageIdentifier.isPresent(), ErrorCode.INVALID_EXPORT, "Missing package path");
        checkFatal(packageIdentifier.isPresent(), ErrorCode.INVALID_EXPORT, "Missing table");
        return new SqrlExportStatement(tableName, packageIdentifier);
      } else {
        //This should not happen
        throw new UnsupportedOperationException("Unexpected import/export directive: " + directive);
      }

    }

    //#2: SQRL Table Definitions
    Matcher sqrlDefinition = SQRL_DEFINITION.matcher(statement);
    if (sqrlDefinition.find()) { //it's a SQRL definition
      ParsedObject<NamePath> tableName = parseNamePath(sqrlDefinition.group("tablename"),
          statement, sqrlDefinition.start("tablename"));
      int commentEnd = sqrlDefinition.start("fullmatch");
      SqrlComments comment = SqrlComments.parse(parse(statement.substring(0,commentEnd), statement, 0));
      ParsedObject<String> definitionBody = parse(statement.substring(sqrlDefinition.end()), statement,
          sqrlDefinition.end());

      AccessModifier access = AccessModifier.QUERY;
      if (tableName.get().getLast().isHidden()) access = AccessModifier.HIDDEN;

      ParsedObject<String> arguments = parse(sqrlDefinition, "arguments", statement);
      //Identify SQL keyword
      Pattern sqlKeywordPattern = Pattern.compile("^\\s*(\\w+)");
      Matcher matcher = sqlKeywordPattern.matcher(definitionBody.get());
      checkFatal(matcher.find(), ErrorCode.INVALID_SQRL_DEFINITION, "Could not parse SQRL statement");
      String keyword = matcher.group(1).trim();
      int keywordEnd = matcher.end(1);
      SqrlDefinition definition = null;
      if (keyword.equalsIgnoreCase(SELECT_KEYWORD) || keyword.equalsIgnoreCase(JOIN_KEYWORD)
          || keyword.equalsIgnoreCase(SUBSCRIBE_KEYWORD)) {
        if (keyword.equalsIgnoreCase(SUBSCRIBE_KEYWORD)) { //Remove the keyword from definition body
          checkFatal(access!=AccessModifier.HIDDEN, ErrorCode.INVALID_SQRL_DEFINITION, "Cannot subscribe to hidden table: %s", tableName.get());
          access = AccessModifier.SUBSCRIPTION;
          int newDefinitionStart = sqrlDefinition.end() + keywordEnd;
          definitionBody = parse(statement.substring(newDefinitionStart), statement, newDefinitionStart);
        }

        if (arguments.isEmpty()) {
          if (keyword.equalsIgnoreCase(JOIN_KEYWORD)) {
            definition = new SqrlRelationshipStatement(tableName, definitionBody, comment,
                Map.of(), List.of());
          } else {
            definition = new SqrlTableDefinition(tableName, definitionBody, access, comment);
          }
        } else {
          //Parse arguments
          Matcher argMatcher = ARGUMENT_PARSER.matcher(arguments.get());
          Map<Name, ParsedObject<String>> argumentMap = new LinkedHashMap<>();
          int lastMatchEnd = 0;
          while (argMatcher.find()) {
            checkFatal(lastMatchEnd==argMatcher.start(), relativeLocation(arguments, lastMatchEnd), ErrorCode.INVALID_TABLE_FUNCTION_ARGUMENTS, "Argument list contains invalid arguments");
            ParsedObject<Name> argName = arguments.fromOffset(parseName(argMatcher, "name", arguments.get()));
            checkFatal(argName.isPresent(), argName.getFileLocation(), ErrorCode.INVALID_TABLE_FUNCTION_ARGUMENTS, "Invalid argument name");
            ParsedObject<String> typeName = arguments.fromOffset(parse(argMatcher, "type", arguments.get()));
            checkFatal(typeName.isPresent(), typeName.getFileLocation(), ErrorCode.INVALID_TABLE_FUNCTION_ARGUMENTS, "Invalid argument type");
            argumentMap.put(argName.get(), typeName);
            lastMatchEnd = argMatcher.end();
          }
          checkFatal(lastMatchEnd==arguments.get().length(), relativeLocation(arguments, lastMatchEnd), ErrorCode.INVALID_TABLE_FUNCTION_ARGUMENTS, "Argument list contains invalid arguments");
          Pair<String, List<Name>> processedBody = replaceTableFunctionVariables(definitionBody.get(), List.copyOf(argumentMap.keySet()));
          definitionBody = definitionBody.map(x -> processedBody.getKey());
          if (keyword.equalsIgnoreCase(JOIN_KEYWORD)) {
            definition = new SqrlRelationshipStatement(tableName, definitionBody, comment,
                argumentMap, processedBody.getRight());
          } else {
            definition = new SqrlTableFunctionStatement(tableName, definitionBody, access, comment,
                argumentMap, processedBody.getRight());
          }
        }
      } else if (keyword.equalsIgnoreCase(DISTINCT_KEYWORD)) {
        checkFatal(arguments.isEmpty(), ErrorCode.INVALID_SQRL_DEFINITION, "Table function not supported for operation");
        Matcher distinctMatcher = DISTINCT_PARSER.matcher(definitionBody.get());
        checkFatal(distinctMatcher.find(), ErrorCode.INVALID_SQRL_DEFINITION, "Could not parse [DISTINCT] statement.");
        ParsedObject<NamePath> from = definitionBody.fromOffset(parseNamePath(distinctMatcher, "from", definitionBody.get()));
        ParsedObject<String> columns = definitionBody.fromOffset(parse(distinctMatcher, "columns", definitionBody.get()));
        ParsedObject<String> remaining = definitionBody.fromOffset(parse(distinctMatcher, "remaining", definitionBody.get()));
        definition = new SqrlDistinctStatement(tableName, comment, access, from, columns, remaining);
      } else {
        //We assume it's a column expression
        checkFatal(arguments.isEmpty(), ErrorCode.INVALID_SQRL_DEFINITION, "Column definitions do not support arguments");
        definition = new SqrlAddColumnStatement(tableName, definitionBody, comment);
        new StatementParserException(ErrorCode.INVALID_SQRL_DEFINITION, definitionBody.getFileLocation(), "Not a valid SQRL operation: %s", keyword);
      }

      return definition;
    }

    //#3: Create Table
    Matcher createTable = CREATE_TABLE.matcher(statement);
    if (createTable.find()) {
      ParsedObject<String> createTableStmt = parse(statement.substring(createTable.start("fullmatch")), statement,
          createTable.start("fullmatch"));
      int commentEnd = createTable.start("fullmatch");
      SqrlComments comment = SqrlComments.parse(parse(statement.substring(0,commentEnd), statement, 0));
      return new SqrlCreateTableStatement(createTableStmt, comment);
    }

    //#4: If none of the regex match, we assume it's a Flink SQL statement
    return new FlinkSQLStatement(new ParsedObject<>(statement, FileLocation.START));
  }

  public static Pair<String, List<Name>> replaceTableFunctionVariables(String body, List<Name> arguments) {
    List<Name> argumentIndexes = new ArrayList<>();
    String bodyLower = body.toLowerCase();
    StringBuilder resultBuilder = new StringBuilder();
    int i = 0;
    while (i < body.length()) {
      boolean matched = false;
      if (body.charAt(i) == ARGUMENT_PREFIX) {
        for (Name argument : arguments) {
          if (bodyLower.startsWith(argument.getCanonical(), i+1)) {
            argumentIndexes.add(argument);
            resultBuilder.append("?");
            for (int k = 0; k < argument.length(); k++) {
              resultBuilder.append(" ");
            }
            i += argument.length()+1;
            matched = true;
            break;
          }
        }
      }
      if (!matched) {
        resultBuilder.append(body.charAt(i));
        i++;
      }
    }
    return Pair.of(resultBuilder.toString(), argumentIndexes);
  }


  static FileLocation relativeLocation(ParsedObject<String> base, int charOffset) {
    return base.getFileLocation().add(computeFileLocation(base.get(), charOffset));
  }

  static FileLocation computeFileLocation(String statement, int charOffset) {
    return SqlScriptStatementSplitter.computeOffset(statement, charOffset);
  }

  static ParsedObject<String> parse(Matcher matcher, String groupName, String statement) {
    String content = matcher.group(groupName);
    return parse(content, statement, content!=null?matcher.start(groupName):0);
  }

  static ParsedObject<String> parse(String content, String statement, int charOffset) {
    if (content==null) return new ParsedObject<>(null, FileLocation.START);

    FileLocation fileLocation = computeFileLocation(statement, charOffset);
    return new ParsedObject<>(content, fileLocation);
  }

  static ParsedObject<NamePath> parseNamePath(String identifier, String statement, int charOffset) {
    return parse(identifier, statement, charOffset).map(NamePath::parse);
  }

  static ParsedObject<NamePath> parseNamePath(Matcher matcher, String groupName, String statement) {
    return parse(matcher, groupName, statement).map(NamePath::parse);
  }

  static ParsedObject<Name> parseName(Matcher matcher, String groupName, String statement) {
    return parse(matcher, groupName, statement).map(Name::system);
  }

}
