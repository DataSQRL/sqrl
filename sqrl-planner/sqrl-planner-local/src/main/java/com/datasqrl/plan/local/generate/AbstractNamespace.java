package com.datasqrl.plan.local.generate;

import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.module.FunctionNamespaceObject;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.schema.SQRLTable;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.commons.lang3.tuple.Pair;
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

  Optional<TableSink> getMutationTable(APISource source, Name name);

  Optional<Pair<TableSource, SQRLTable>> getSubscription(APISource source, Name name);


  java.util.Set<java.net.URL> getJars();
}
