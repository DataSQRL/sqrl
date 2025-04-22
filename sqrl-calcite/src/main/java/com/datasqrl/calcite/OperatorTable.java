package com.datasqrl.calcite;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

import com.google.inject.Inject;

public class OperatorTable implements SqlOperatorTable {

  private final SqlOperatorTable[] chain;
  private final SqrlSchema schema;

  @Inject
  public OperatorTable(CatalogReader catalogReader, SqrlSchema schema) {
    this.chain = new SqlOperatorTable[]{catalogReader, FlinkSqlOperatorTable.instance(false)};
    this.schema = schema;
  }

  public OperatorTable(SqrlSchema schema, SqlOperatorTable chain) {
    this.chain = new SqlOperatorTable[]{chain};
    this.schema = schema;
  }

  @Override
  public void lookupOperatorOverloads(SqlIdentifier sqlIdentifier, SqlFunctionCategory sqlFunctionCategory, SqlSyntax sqlSyntax, List<SqlOperator> list, SqlNameMatcher sqlNameMatcher) {
    //Support aliasing instead of converting functions
    if (sqlIdentifier.names.size() == 1 &&
        schema.getFncAlias().containsKey(sqlIdentifier.names.get(0).toLowerCase())) {
      sqlIdentifier = new SqlIdentifier(schema.getFncAlias().get(sqlIdentifier.names.get(0).toLowerCase()),
          sqlIdentifier.getParserPosition());
    }

    for (SqlOperatorTable table : chain) {
      table.lookupOperatorOverloads(sqlIdentifier, sqlFunctionCategory, sqlSyntax, list, sqlNameMatcher);
    }

    //dedupe
    List dedup = new ArrayList<>(new HashSet<>(list));
    list.clear();
    list.addAll(dedup);
  }

  @Override
  public List<SqlOperator> getOperatorList() {
    return new ArrayList<>();
  }
}
