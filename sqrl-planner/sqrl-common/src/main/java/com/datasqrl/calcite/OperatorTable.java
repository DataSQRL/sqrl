package com.datasqrl.calcite;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.validate.SqlNameMatcher;

import java.util.*;

public class OperatorTable implements SqlOperatorTable {
  private final Map<String, SqlOperator> udf = new HashMap<>();
  private final Map<List<String>, SqlOperator> udfListMap = new HashMap<>();
  private final Map<List<String>, SqlOperator> internalNames = new HashMap<>();
  private final NameCanonicalizer nameCanonicalizer;
  private final SqlOperatorTable[] chain;

  public OperatorTable(NameCanonicalizer nameCanonicalizer, SqlOperatorTable... chain) {
    this.nameCanonicalizer = nameCanonicalizer;
    this.chain = chain;
  }

  @Override
  public void lookupOperatorOverloads(SqlIdentifier sqlIdentifier, SqlFunctionCategory sqlFunctionCategory, SqlSyntax sqlSyntax, List<SqlOperator> list, SqlNameMatcher sqlNameMatcher) {
    if (list.isEmpty()) {
      SqlOperator fn = sqlNameMatcher.get(udfListMap, List.of(), List.of(sqlIdentifier.getSimple()));
      if (fn != null) {
        list.add(fn);
      }
    }

    //Also check the function name since calcite will convert to their function name
    if (list.isEmpty()) {
      SqlOperator fn = sqlNameMatcher.get(internalNames, List.of(), List.of(sqlIdentifier.getSimple()));
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
    return new ArrayList<>(this.udf.values());
  }

  public void addFunction(String canonicalName, SqlOperator function) {
    this.udf.put(nameCanonicalizer.getCanonical(canonicalName), function);
    this.udfListMap.put(List.of(nameCanonicalizer.getCanonical(canonicalName)), function);
    this.internalNames.put(List.of(function.getName()), function);
  }

  public Map<String, SqlOperator> getUdfs() {
    return udf;
  }
}
