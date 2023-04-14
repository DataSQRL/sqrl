package com.datasqrl.plan.local.generate;

import com.datasqrl.module.FunctionNamespaceObject;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.canonicalizer.Name;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.flink.table.functions.UserDefinedFunction;

public interface AbstractNamespace {

  boolean addNsObject(NamespaceObject nsObject);

  boolean addNsObject(Name name, NamespaceObject nsObject);

  /**
   * Adds the given {@link FunctionNamespaceObject} to the namespace.
   *
   * @param name
   * @param nsObject the {@link FunctionNamespaceObject} to add
   * @return {@code true} if the object was successfully added; {@code false} otherwise.
   */
  public boolean addFunctionObject(Name name,
      FunctionNamespaceObject<UserDefinedFunction> nsObject);

  Optional<SqlFunction> translateFunction(Name name);

  SqlOperatorTable getOperatorTable();

  SqrlSchema getSchema();

  List<ResolvedExport> getExports();

  void addExport(ResolvedExport resolvedExport);

  java.util.Set<java.net.URL> getJars();
}
