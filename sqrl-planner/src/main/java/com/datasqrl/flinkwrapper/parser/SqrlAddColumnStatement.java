package com.datasqrl.flinkwrapper.parser;

import static com.datasqrl.flinkwrapper.parser.SqlScriptStatementSplitter.addStatementDelimiter;
import static com.datasqrl.flinkwrapper.parser.SqlScriptStatementSplitter.removeStatementDelimiter;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.flinkwrapper.Sqrl2FlinkSQLTranslator;
import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.commons.collections4.ListUtils;

public class SqrlAddColumnStatement extends SqrlDefinition implements StackableStatement {

  public static final String ALTER_VIEW_PREFIX = "ALTER VIEW %s AS ";
  public static final String ADD_COLUMN_PREFIX = "SELECT *, ";
  public static final String ADD_COLUMN_SQL = ADD_COLUMN_PREFIX + "%s AS %s FROM \n( %s )";

  public SqrlAddColumnStatement(ParsedObject<NamePath> tableName,
      ParsedObject<String> definitionBody,
      SqrlComments comments) {
    super(tableName, definitionBody, comments);
  }

  public String toSql(Sqrl2FlinkSQLTranslator sqrlEnv, List<StackableStatement> stack) {
    StatementParserException.checkFatal(tableName.get().size()>1, tableName.getFileLocation(), ErrorLabel.GENERIC,
        "Column expression requires column name: %s", tableName.get());
    NamePath table = this.tableName.get().popLast();
    StatementParserException.checkFatal(!stack.isEmpty() &&
        stack.get(0) instanceof SqrlTableDefinition && ((SqrlTableDefinition)stack.get(0)).tableName.get().equals(table),
        this.tableName.getFileLocation(), ErrorLabel.GENERIC,
        "Column expression must directly follow the definition of table [%s]", table);
    //It's guaranteed that all other entries in the stack must be add-column statements on the same table
    String innerSql = removeStatementDelimiter(((SqrlTableDefinition)stack.get(0)).definitionBody.get());
    for (StackableStatement col : ListUtils.union(stack.subList(1, stack.size()), List.of(this))) {
      Preconditions.checkArgument(col instanceof SqrlAddColumnStatement);
      SqrlAddColumnStatement column = (SqrlAddColumnStatement) col;
      String columName = column.tableName.get().getLast().getDisplay();
      innerSql = addColumn(columName, removeStatementDelimiter(column.definitionBody.get()), innerSql);
    }
    String sql = String.format(ALTER_VIEW_PREFIX + "%s", table.toString(), addStatementDelimiter(innerSql));
    return sql;
  }

  String getPrefix() {
    return String.format(ALTER_VIEW_PREFIX + ADD_COLUMN_PREFIX, this.tableName.get().popLast());
  }

  @Override
  public FileLocation mapSqlLocation(FileLocation location) {
    return definitionBody.getFileLocation().add(
        SQLStatement.removeFirstRowOffset(location, getPrefix().length()));
  }

  private static String addColumn(String columnName, String columnExpression, String innerBody) {
    return String.format(ADD_COLUMN_SQL, columnExpression, columnName, innerBody);
  }

  @Override
  public boolean isRoot() {
    return false;
  }
}
