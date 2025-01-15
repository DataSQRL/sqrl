package com.datasqrl.flinkwrapper.parser;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.flinkwrapper.Sqrl2FlinkSQLTranslator;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.table.operations.Operation;

/**
 * A partially parsed SQRL definition. Some elements are extracted but the
 * body of the definition is kept as a string to be passed to the Flink parser
 * for parsing and conversion.
 *
 * As such, we try to do our best to keep offsets to map errors back by preserving position.
 */
@AllArgsConstructor
public abstract class SqrlDefinition implements SqrlStatement {

  final ParsedObject<NamePath> tableName;
  final ParsedObject<String> definitionBody;
  final SqrlComments comments;

  public ParsedSql toSqlNode(Sqrl2FlinkSQLTranslator sqrlEnv, List<StackableStatement> stack) {
    String prefix = String.format("CREATE VIEW %s AS ", tableName.get().toString());
    String sql = prefix + definitionBody.get();
    SqlNode sqlNode = StatementParserException.handleParseErrors(sqrlEnv::parseSQL, sql,
        definitionBody.getFileLocation(), prefix.length());
    return new ParsedSql(sqlNode, sql);
  }

}
