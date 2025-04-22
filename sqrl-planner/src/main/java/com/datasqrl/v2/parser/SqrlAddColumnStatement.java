package com.datasqrl.v2.parser;

import static com.datasqrl.v2.parser.SqlScriptStatementSplitter.addStatementDelimiter;
import static com.datasqrl.v2.parser.SqlScriptStatementSplitter.removeStatementDelimiter;

import java.util.List;

import org.apache.commons.collections4.ListUtils;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.v2.Sqrl2FlinkSQLTranslator;
import com.google.common.base.Preconditions;

/**
 * Represents a column definition that extends a previous table definition
 */
public class SqrlAddColumnStatement extends SqrlDefinition implements StackableStatement {

  public static final String ALTER_VIEW_PREFIX = "ALTER VIEW %s AS ";
  public static final String ADD_COLUMN_PREFIX = "SELECT *, ";
  public static final String ADD_COLUMN_SQL = ADD_COLUMN_PREFIX + "%s AS %s FROM \n( %s )";

  public SqrlAddColumnStatement(ParsedObject<NamePath> tableName,
      ParsedObject<String> definitionBody,
      SqrlComments comments) {
    super(tableName, definitionBody, AccessModifier.INHERIT, comments);
  }

  @Override
public String toSql(Sqrl2FlinkSQLTranslator sqrlEnv, List<StackableStatement> stack) {
    StatementParserException.checkFatal(tableName.get().size()>1, tableName.getFileLocation(), ErrorLabel.GENERIC,
        "Column expression requires column name: %s", tableName.get());
    var table = this.tableName.get().popLast();

    //It's guaranteed that all other entries in the stack must be add-column statements on the same table
    var innerSql = removeStatementDelimiter(((SqrlTableDefinition)stack.get(0)).definitionBody.get());
    for (StackableStatement col : ListUtils.union(stack.subList(1, stack.size()), List.of(this))) {
      Preconditions.checkArgument(col instanceof SqrlAddColumnStatement);
      var column = (SqrlAddColumnStatement) col;
      var columName = column.tableName.get().getLast().getDisplay();
      innerSql = addColumn(columName, removeStatementDelimiter(column.definitionBody.get()), innerSql);
    }
    var sql = String.format(ALTER_VIEW_PREFIX + "%s", table.toString(), addStatementDelimiter(innerSql));
    return sql;
  }

  @Override
  String getPrefix() {
    return String.format(ALTER_VIEW_PREFIX + ADD_COLUMN_PREFIX, this.tableName.get().popLast());
  }

  @Override
  public FileLocation mapSqlLocation(FileLocation location) {
    return definitionBody.getFileLocation().add(
        SQLStatement.removeFirstRowOffset(location, getPrefix().length()));
  }

  private static String addColumn(String columnName, String columnExpression, String innerBody) {
    return ADD_COLUMN_SQL.formatted(columnExpression, columnName, innerBody);
  }

  @Override
  public boolean isRoot() {
    return false;
  }
}
