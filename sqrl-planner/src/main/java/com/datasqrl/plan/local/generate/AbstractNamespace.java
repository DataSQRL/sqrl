package com.datasqrl.plan.local.generate;

import com.datasqrl.error.ErrorCode;
import com.datasqrl.loaders.TableSourceNamespaceObject;
import com.datasqrl.name.Name;
import com.datasqrl.plan.local.ScriptTableDefinition;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.tools.RelBuilder;
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

  public boolean addTableObject(Name name, TableNamespaceObject nsObject);

  void registerScriptTable(ScriptTableDefinition tblDef);

  SqlOperatorTable getOperatorTable();

  SqrlCalciteSchema getSchema();

  RelBuilder createRelBuilder();

  List<ResolvedExport> getExports();

  void addExport(ResolvedExport resolvedExport);

  java.util.Set<java.net.URL> getJars();
}
