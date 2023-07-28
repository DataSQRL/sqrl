package com.datasqrl.functions;


import com.datasqrl.canonicalizer.Name;
import com.datasqrl.functions.flink.FunctionConverter;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class SqrlOperatorTable implements SqlOperatorTable {
  private Map<String, SqlFunction> udf = new CaseInsensitiveMap();

  @Override
  public void lookupOperatorOverloads(SqlIdentifier sqlIdentifier, SqlFunctionCategory sqlFunctionCategory, SqlSyntax sqlSyntax, List<SqlOperator> list, SqlNameMatcher sqlNameMatcher) {
    SqlFunction fn = udf.get(sqlIdentifier.getSimple());
    if (fn != null) {
      list.add(fn);
    }
  }

  @Override
  public List<SqlOperator> getOperatorList() {
    return new ArrayList<>(this.udf.values());
  }

  public void addFlinkFunction(String name, FunctionDefinition function) {
    //this doesn't belong here

    this.udf.put(name, FunctionConverter.convert(name, function));
  }

  public void addSystemFunction(Name name, SqlFunction function) {
    //todo: system name functions are still accessible in sqrl script but shouldn't be.
    // transpiler will handle types during analysis soon so we can check it then
    this.udf.put(name.getDisplay(), function);
    this.udf.put(function.getName(), function);
  }

  public void addFlinkFunction(BuiltInFunctionDefinition f) {
    addFlinkFunction(f.getName(), f);
  }
}
