package com.datasqrl.flinkwrapper.parser;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.flinkwrapper.Sqrl2FlinkSQLTranslator;
import java.util.List;

public class SqrlDistinctStatement extends SqrlDefinition {

  public SqrlDistinctStatement(
      ParsedObject<NamePath> tableName,
      SqrlComments comments,
      ParsedObject<NamePath> from,
      ParsedObject<String> columns,
      ParsedObject<String> remaining) {
    super(tableName,
        new ParsedObject<>(String.format("SELECT %s FROM (SELECT * FROM %s ORDER BY %s)",
                                          columns.get(), from.get(), remaining.get()),
            columns.getFileLocation()),
        comments);
  }

  public static final String FILTERED_DISTINCT_HINT_NAME = "filtered_distinct_order";


  public String toSql(Sqrl2FlinkSQLTranslator sqrlEnv, List<StackableStatement> stack) {
    String sql = super.toSql(sqrlEnv, stack);
    //Rewrite statement
    boolean isFilteredDistinct = comments.getHints().stream().filter(ParsedObject::isPresent)
        .map(ParsedObject::get).anyMatch(hint -> hint.getName().equalsIgnoreCase(FILTERED_DISTINCT_HINT_NAME));

    /*TODO: (this should not require writing new code, just copying/rearranging existing code)
    0) Convert to Relnode: RelRoot relRoot = sqrlEnv.toRelRoot(sql);
    1) Extract code from TopN conversion to create deduplication SQL: AnnotatedLP#inlineTopN
    2) If filtered distinct and order by is not by ROWTIME, add LAG using the Calcite util method: CalciteUtil#addFilteredDeduplication
    3) Convert the result back to SQLNode and return: SqrlToFlinkSQLGenerator but since this is "simple" SQL a simple unparse might do
     */

    return sql;
  }

}
