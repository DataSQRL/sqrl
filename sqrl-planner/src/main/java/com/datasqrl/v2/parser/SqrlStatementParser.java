package com.datasqrl.v2.parser;

import static com.datasqrl.v2.parser.StatementParserException.checkFatal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.catalog.ObjectIdentifier;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.v2.hint.PlannerHints;
import com.datasqrl.v2.parser.SqrlTableFunctionStatement.ParsedArgument;
import com.google.inject.Inject;

import lombok.AllArgsConstructor;

/**
 * A lightweight REGEX based parser to identify and parse SQRL specific SQL statements.
 *
 * The goal is to pass most of the actual parsing into the Flink parser to remain compatible with Flink and
 * make it easier to maintain that compatibility. That's why we are using a REGEX based approach to wrap around
 * the Calcite parser that Flink uses.
 * We tried building our own parser in the past, but that created too much work trying to maintain compatibility.
 * We might consider extending Flink's parser in the future, but since Flink evovles quickly this is the safer bet for now.
 *
 *
 * Most of this code is defining and matching REGEX patterns.
 * A particular challenge is maintaining the line+column offsets to correctly pinpoint the source of parser errors. A lot of the
 * code and complexity in this implementation is due to that. We use {@link ParsedObject} to keep file locations.
 * This is handled through {@link ParsedObject}.
 */
@AllArgsConstructor(onConstructor_=@Inject)
public class SqrlStatementParser {

  public static final String IDENTIFIER_REGEX = "[\\w\\.`*-]+?";
  public static final String COMMENT_REGEX = "(/\\*)+?[\\w\\W]*?(\\*/)";
  public static final String BEGINNING_COMMENT = "^(("+ COMMENT_REGEX +")|(\\s*))*";
  public static final String IMPORT_EXPORT_REGEX = BEGINNING_COMMENT + "(?<fullmatch>import|export)";
  public static final String SQRL_DEFINITION_REGEX = BEGINNING_COMMENT + "(?<fullmatch>(?<tablename>"+IDENTIFIER_REGEX+")\\s*(\\((?<arguments>.*?)\\))?\\s*:=)";
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
  public static final String DISTINCT_KEYWORD = "distinct";
  public static final String SUBSCRIBE_KEYWORD = "subscribe";

  public static final String SELF_REFERENCE_KEYWORD = "this";

  public static final String VARIABLE_REGEX = "(?<prefix>\\W)(?<type>:|@|"+SELF_REFERENCE_KEYWORD+"\\.)((?<name1>\\w+)|`(?<name2>[^` ]+)`)";
  public static final Pattern VARIABLE_PARSER = Pattern.compile(VARIABLE_REGEX, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private SqlScriptStatementSplitter sqlSplitter;

  /**
   * Main entry point for parsing a script
   * @param script
   * @param scriptErrors
   * @return
   */
  public List<ParsedObject<SQLStatement>> parseScript(String script, ErrorCollector scriptErrors) {
    List<ParsedObject<SQLStatement>> sqlStatements = new ArrayList<>();
    var localErrors = scriptErrors;
    try {
      var statements = sqlSplitter.splitStatements(script);
      for (ParsedObject<String> statement : statements) {
        localErrors = scriptErrors.atFile(statement.getFileLocation());
        sqlStatements.add(new ParsedObject<>(parseStatement(statement.getObject()), statement.getFileLocation()));
     }
    } catch (StatementParserException e) {
      throw localErrors.handle(e);
    }
    return sqlStatements;
  }

  /**
   * Parses an individual statement
   *
   * @param statement
   * @return
   */
  public SQLStatement parseStatement(String statement) {
    var importExportMatcher = IMPORT_EXPORT.matcher(statement);

    //#1: Imports and Exports
    if (importExportMatcher.find()) { //it's an import or export statement
      var directive = importExportMatcher.group("fullmatch");
      var commentEnd = importExportMatcher.end()-directive.length();
      var comment = SqrlComments.parse(parse(statement.substring(0,commentEnd), statement, 0));
      var body = statement.substring(importExportMatcher.end()).trim();

      if (directive.equalsIgnoreCase("import")) {
        var subMatcher = IMPORT_PARSER.matcher(body);
        checkFatal(subMatcher.find(), ErrorCode.INVALID_IMPORT, "Could not parse IMPORT statement");
        var packageIdentifier = parseNamePath(subMatcher, "package", statement);
        var alias = parseNamePath(subMatcher, "identifier", statement);
        checkFatal(packageIdentifier.isPresent(), ErrorCode.INVALID_IMPORT, "Missing package path");
        return new SqrlImportStatement(packageIdentifier, alias, comment);
      } else if (directive.equalsIgnoreCase("export")) {
        var subMatcher = EXPORT_PARSER.matcher(body);
        checkFatal(subMatcher.find(), ErrorCode.INVALID_IMPORT, "Could not parse IMPORT statement");
        var packageIdentifier = parseNamePath(subMatcher, "package", statement);
        var tableName = parseNamePath(subMatcher, "identifier", statement);
        checkFatal(packageIdentifier.isPresent(), ErrorCode.INVALID_EXPORT, "Missing package path");
        checkFatal(packageIdentifier.isPresent(), ErrorCode.INVALID_EXPORT, "Missing table");
        return new SqrlExportStatement(tableName, packageIdentifier, comment);
      } else {
        //This should not happen
        throw new UnsupportedOperationException("Unexpected import/export directive: " + directive);
      }

    }

    //#2: SQRL Table Definitions
    var sqrlDefinition = SQRL_DEFINITION.matcher(statement);
    if (sqrlDefinition.find()) { //it's a SQRL definition
      var tableName = parseNamePath(sqrlDefinition.group("tablename"),
          statement, sqrlDefinition.start("tablename"));
      checkFatal(tableName.isPresent(), tableName.getFileLocation(), ErrorCode.INVALID_SQRL_DEFINITION, "Invalid name for definition");
      checkFatal(tableName.get().size()<=2, tableName.getFileLocation(), ErrorCode.INVALID_SQRL_DEFINITION, "Invalid name for definition: %s", tableName.get());
      var isRelationship = tableName.get().size()>1;
      var commentEnd = sqrlDefinition.start("fullmatch");
      var comment = SqrlComments.parse(parse(statement.substring(0,commentEnd), statement, 0));
      var definitionBody = parse(statement.substring(sqrlDefinition.end()), statement,
          sqrlDefinition.end());

      var access = AccessModifier.QUERY;

      ParsedObject<String> arguments = parse(sqrlDefinition, "arguments", statement)
          .map(str -> str.isBlank()?null:str);
      //Identify SQL keyword
      var sqlKeywordPattern = Pattern.compile("^\\s*(\\w+)");
      var keywordMatcher = sqlKeywordPattern.matcher(definitionBody.get());
      var keyword = "";
      var keywordEnd = 0;
      if (keywordMatcher.find()) {
        keyword = keywordMatcher.group(1).trim();
        keywordEnd = keywordMatcher.end(1);
      }
      SqrlDefinition definition = null;
      if (keyword.equalsIgnoreCase(SELECT_KEYWORD)
          || keyword.equalsIgnoreCase(SUBSCRIBE_KEYWORD)) {
        if (keyword.equalsIgnoreCase(SUBSCRIBE_KEYWORD)) { //Remove the keyword from definition body
          checkFatal(!isRelationship, tableName.getFileLocation(), ErrorCode.INVALID_SQRL_DEFINITION, "Cannot subscribe for a relationship");
          access = AccessModifier.SUBSCRIPTION;
          var newDefinitionStart = sqrlDefinition.end() + keywordEnd;
          definitionBody = parse(statement.substring(newDefinitionStart), statement, newDefinitionStart);
        }
        if (arguments.isEmpty() && !isRelationship) {
            definition = new SqrlTableDefinition(tableName, definitionBody, access, comment);
        } else {
          //Extract and replace argument and this references
          var processedBody = replaceTableFunctionVariables(definitionBody, isRelationship);
          definitionBody = definitionBody.map(x -> processedBody.getKey());
          definition = new SqrlTableFunctionStatement(tableName, definitionBody, access, comment,
              arguments, processedBody.getRight());
        }
      } else if (keyword.equalsIgnoreCase(DISTINCT_KEYWORD)) { //SQRL's special DISTINCT statement
        checkFatal(arguments.isEmpty(), ErrorCode.INVALID_SQRL_DEFINITION, "Table function not supported for operation");
        var distinctMatcher = DISTINCT_PARSER.matcher(definitionBody.get());
        checkFatal(distinctMatcher.find(), ErrorCode.INVALID_SQRL_DEFINITION, "Could not parse [DISTINCT] statement.");
        ParsedObject<NamePath> from = definitionBody.fromOffset(parseNamePath(distinctMatcher, "from", definitionBody.get()));
        ParsedObject<String> columns = definitionBody.fromOffset(parse(distinctMatcher, "columns", definitionBody.get()));
        ParsedObject<String> remaining = definitionBody.fromOffset(parse(distinctMatcher, "remaining", definitionBody.get()));
        definition = new SqrlDistinctStatement(tableName, comment, access, from, columns, remaining);
      } else { //otherwise, we assume it's a column expression
        checkFatal(arguments.isEmpty(), ErrorCode.INVALID_SQRL_DEFINITION, "Column definitions do not support arguments");
        definition = new SqrlAddColumnStatement(tableName, definitionBody, comment);
        new StatementParserException(ErrorCode.INVALID_SQRL_DEFINITION, definitionBody.getFileLocation(), "Not a valid SQRL operation: %s", keyword);
      }

      return definition;
    }

    //#3: Create Table
    var createTable = CREATE_TABLE.matcher(statement);
    if (createTable.find()) {
      var createTableStmt = parse(statement.substring(createTable.start("fullmatch")), statement,
          createTable.start("fullmatch"));
      var commentEnd = createTable.start("fullmatch");
      var comment = SqrlComments.parse(parse(statement.substring(0,commentEnd), statement, 0));
      return new SqrlCreateTableStatement(createTableStmt, comment);
    }

    //#4: If none of the regex match, we assume it's a Flink SQL statement
    return new FlinkSQLStatement(new ParsedObject<>(statement, FileLocation.START));
  }

  /**
   * For the body of function definitions, we need to replace the argument names (which start with '$')
   * with just a '?' so Calcite can parse them as dynamic parameters. We also keep track of the index of the match
   * so we can later (in {@link com.datasqrl.v2.Sqrl2FlinkSQLTranslator#resolveSqrlTableFunction(ObjectIdentifier, String, List, Map, PlannerHints, ErrorCollector)}
   * map the dynamic parameters back to the arguments by index. This additional complexity is necessary
   * because Calcite does not support indexes or names for dynamic parameters so we have to do this manually and
   * keep track of the mapping between the parser and the planner.
   *
   *
   * @param body
   * @param isRelationship
   * @return
   */
  public static Pair<String, List<ParsedArgument>> replaceTableFunctionVariables(ParsedObject<String> body, boolean isRelationship) {
    List<ParsedArgument> argumentIndexes = new ArrayList<>();
    var matcher = VARIABLE_PARSER.matcher(body.get());
    var processedQuery = new StringBuilder();

    while (matcher.find()) {
      var variableType = matcher.group("type");
      var isParentField = false;
      if (variableType.toLowerCase().startsWith(SELF_REFERENCE_KEYWORD)) {
        checkFatal(isRelationship, body.getFileLocation().add(computeFileLocation(body.get(), matcher.start("type"))), ErrorCode.INVALID_SQRL_DEFINITION,
              "Reserved reference `this` can only be used in relationship definitions");
        isParentField = true;
      }
      String matchedVariable;
      int startPos;
      if (matcher.group("name1") != null) {
        matchedVariable = matcher.group("name1");
        startPos = matcher.start("name1");
      } else {
        matchedVariable = matcher.group("name2");
        startPos = matcher.start("name2");
      }
      var variable = new ParsedObject<String>(matchedVariable, body.getFileLocation().add(computeFileLocation(body.get(), startPos)));
      var replacement = matcher.group("prefix") + "?" + " ".repeat(matcher.end()-matcher.start()-2);
      matcher.appendReplacement(processedQuery, replacement);
      var arg = new ParsedArgument(variable, isParentField, argumentIndexes.size());
      argumentIndexes.add(arg);
    }
    matcher.appendTail(processedQuery);
    return Pair.of(processedQuery.toString(), argumentIndexes);
  }

  static FileLocation relativeLocation(ParsedObject<String> base, int charOffset) {
    return base.getFileLocation().add(computeFileLocation(base.get(), charOffset));
  }

  static FileLocation computeFileLocation(String statement, int charOffset) {
    return SqlScriptStatementSplitter.computeOffset(statement, charOffset);
  }

  static ParsedObject<String> parse(Matcher matcher, String groupName, String statement) {
    var content = matcher.group(groupName);
    return parse(content, statement, content!=null?matcher.start(groupName):0);
  }

  static ParsedObject<String> parse(String content, String statement, int charOffset) {
    if (content==null) {
		return new ParsedObject<>(null, FileLocation.START);
	}

    var fileLocation = computeFileLocation(statement, charOffset);
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
