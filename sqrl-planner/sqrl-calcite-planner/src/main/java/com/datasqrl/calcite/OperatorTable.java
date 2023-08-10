package com.datasqrl.calcite;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.commons.collections.map.CaseInsensitiveMap;

import java.net.URL;
import java.util.*;

public class OperatorTable implements SqlOperatorTable {
  private final Map<String, SqlFunction> udf = new CaseInsensitiveMap();
  private final Set<URL> jars = new HashSet<>();
  private final SqlOperatorTable[] chain;

  public OperatorTable(SqlOperatorTable... chain) {
    this.chain = chain;
  }

  @Override
  public void lookupOperatorOverloads(SqlIdentifier sqlIdentifier, SqlFunctionCategory sqlFunctionCategory, SqlSyntax sqlSyntax, List<SqlOperator> list, SqlNameMatcher sqlNameMatcher) {
    SqlFunction fn = udf.get(sqlIdentifier.getSimple());
    if (fn != null) {
      list.add(fn);
    }

    for (SqlOperatorTable table : chain) {
      table.lookupOperatorOverloads(sqlIdentifier, sqlFunctionCategory, sqlSyntax, list, sqlNameMatcher);
    }
  }

  @Override
  public List<SqlOperator> getOperatorList() {
    return new ArrayList<>(this.udf.values());
  }

  public void addFunction(String canonicalName, SqlFunction function) {
    this.udf.put(canonicalName, function);
  }

  public void addJarUrl(URL j) {
    jars.add(j);
  }

  public Set<URL> getJars() {
    return jars;
  }

  public Map<String, SqlFunction> getUdfs() {
    return udf;
  }

  public OperatorTable forDialect(SqlDialect dialect) {
    RexBuilder f
        ;
    return null;
  }
}
