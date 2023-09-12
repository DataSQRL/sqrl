package com.datasqrl.calcite;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.plan.util.ContinuousIndexMap.Builder;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.commons.collections.map.CaseInsensitiveMap;

import java.net.URL;
import java.util.*;

public class OperatorTable implements SqlOperatorTable {
  private final Map<List<String>, SqlFunction> udf = new HashMap<>();
  private final Map<List<String>, SqlFunction> internalNames = new HashMap<>();
  private final SqlOperatorTable[] chain;
  private final Map<String, SqlFunction> planningFncs = new HashMap<>();

  public OperatorTable(SqlOperatorTable... chain) {
    this.chain = chain;
  }

  @Override
  public void lookupOperatorOverloads(SqlIdentifier sqlIdentifier, SqlFunctionCategory sqlFunctionCategory, SqlSyntax sqlSyntax, List<SqlOperator> list, SqlNameMatcher sqlNameMatcher) {
    if (planningFncs.containsKey(sqlIdentifier.names.get(0))) {
      list.add(planningFncs.get(sqlIdentifier.names.get(0)));
      return;
    }

    if (list.isEmpty()) {
      SqlFunction fn = sqlNameMatcher.get(udf, List.of(), List.of(sqlIdentifier.getSimple()));
      if (fn != null) {
        list.add(fn);
      }
    }

    //Also check the function name since calcite will convert to their function name
    if (list.isEmpty()) {
      SqlFunction fn = sqlNameMatcher.get(internalNames, List.of(), List.of(sqlIdentifier.getSimple()));
      if (fn != null) {
        list.add(fn);
      }
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
    if (this.udf.containsKey(List.of(Name.system(canonicalName).getCanonical()))) {
      throw new RuntimeException(String.format("Function already exists: %s", canonicalName));
    }
    this.udf.put(List.of(canonicalName.toLowerCase()), function);
    this.internalNames.put(List.of(function.getName()), function);
  }

  public Map<List<String>, SqlFunction> getUdfs() {
    return udf;
  }

  public void addPlanningFnc(List<SqlFunction> fncs) {
    fncs.forEach(f->this.planningFncs.put(f.getName(), f));
  }
}