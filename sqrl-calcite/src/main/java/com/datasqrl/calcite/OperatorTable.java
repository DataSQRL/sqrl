package com.datasqrl.calcite;

import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatcher;

public class OperatorTable implements SqlOperatorTable {

  private final SqlOperatorTable[] chain;
  private final SqrlSchema schema;

  @Inject
  public OperatorTable(CatalogReader catalogReader, SqrlSchema schema) {
    this.chain = new SqlOperatorTable[]{catalogReader, SqlStdOperatorTable.instance()};
    this.schema = schema;
  }

  @Override
  public void lookupOperatorOverloads(SqlIdentifier sqlIdentifier, SqlFunctionCategory sqlFunctionCategory, SqlSyntax sqlSyntax, List<SqlOperator> list, SqlNameMatcher sqlNameMatcher) {
    if (list.isEmpty()) {
      SqlOperator fn = sqlNameMatcher.get(schema.getUdfListMap(), List.of(), List.of(sqlIdentifier.getSimple()));
      if (fn != null) {
        list.add(fn);
      }
    }

    //Also check the function name since calcite will convert to their function name
    if (list.isEmpty()) {
      SqlOperator fn = sqlNameMatcher.get(schema.getInternalNames(), List.of(), List.of(sqlIdentifier.getSimple()));
      if (fn != null) {
        list.add(fn);
      }
    }

    //Exit early so we don't get duplicate calcite functions
    if (!list.isEmpty()) {
      return;
    }
    for (SqlOperatorTable table : chain) {
      table.lookupOperatorOverloads(sqlIdentifier, sqlFunctionCategory, sqlSyntax, list, sqlNameMatcher);
    }

  }

  @Override
  public List<SqlOperator> getOperatorList() {
    return new ArrayList<>(schema.getUdf().values());
  }

  public Map<String, SqlOperator> getUdfs() {
    return schema.getUdf();
  }
}
