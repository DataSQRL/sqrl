package com.datasqrl.plan.local.generate;

import com.datasqrl.function.CalciteFunctionNsObject;
import com.datasqrl.function.FlinkUdfNsObject;
import com.datasqrl.loaders.TableSinkNamespaceObject;
import com.datasqrl.loaders.TableSourceNamespaceObject;
import com.datasqrl.loaders.TableSourceSinkNamespaceObject;
import com.datasqrl.module.FunctionNamespaceObject;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.TableNamespaceObject;
import com.datasqrl.canonicalizer.Name;
import com.google.common.base.Preconditions;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.flink.table.functions.UserDefinedFunction;


public class Namespace implements AbstractNamespace {

  private final SqrlSchema schema;

  @Getter
  private Map<String, UserDefinedFunction> udfs = new HashMap<>();

  @Getter
  private Set<URL> jars;

  private List<ResolvedExport> exports = new ArrayList<>();

  private Map<Name, SqlFunction> systemProvidedFunctionMap = new HashMap<>();

  public Namespace(SqrlSchema schema) {
    this.schema = schema;
    this.jars = new HashSet<>();
  }

  /**
   * Adds the given {@link NamespaceObject} to the namespace.
   *
   * @param nsObject the {@link NamespaceObject} to add
   * @return {@code true} if the object was successfully added; {@code false} otherwise.
   */
  @Override
  public boolean addNsObject(NamespaceObject nsObject) {
    return addNsObject(nsObject.getName(), nsObject);
  }

  @Override
  public boolean addNsObject(Name name, NamespaceObject nsObject) {
    if (nsObject instanceof FunctionNamespaceObject) {
      return addFunctionObject(name, (FunctionNamespaceObject) nsObject);
    } else if (nsObject instanceof TableNamespaceObject) {
      return addTableObject(name, (TableNamespaceObject) nsObject);
    } else if (nsObject instanceof TableSourceNamespaceObject) {
      return addTableObject(name, (TableSourceNamespaceObject) nsObject);
    } else if (nsObject instanceof TableSinkNamespaceObject) {
      throw new RuntimeException("Cannot import a sink directly.");
    } else if (nsObject instanceof TableSourceSinkNamespaceObject) {
      return addTableObject(name, (TableSourceSinkNamespaceObject) nsObject);
    } else {
      throw new UnsupportedOperationException("Unexpected namespace object: " + nsObject.getClass());
    }
  }

  public boolean addFunctionObject(Name name, FunctionNamespaceObject nsObject) {
    if (nsObject instanceof CalciteFunctionNsObject) {
      systemProvidedFunctionMap.put(name, ((CalciteFunctionNsObject) nsObject).getFunction());
    } else if (nsObject instanceof FlinkUdfNsObject) {
      FlinkUdfNsObject fctObject = (FlinkUdfNsObject) nsObject;
      udfs.put(name.getCanonical(), fctObject.getFunction());
      schema.addFunction(name.getCanonical(), fctObject.getFunction());
      fctObject.getJarUrl().map(j -> jars.add(j));
    } else {
      throw new UnsupportedOperationException("Unexpected function object: " + nsObject.getClass());
    }
    return true;
  }

  /**
   * Checks if a function translates to a system defined function. Used for aliasing
   * system functions.
   */
  @Override
  public Optional<SqlFunction> translateFunction(Name name) {
    return Optional.ofNullable(systemProvidedFunctionMap.get(name));
  }

  /**
   * Adds the given {@link TableNamespaceObject} to the namespace.
   *
   * @param name
   * @param nsObject the {@link TableNamespaceObject} to add
   * @return {@code true} if the object was successfully added; {@code false} otherwise.
   */
  public boolean addTableObject(Name name, TableNamespaceObject nsObject) {
    Preconditions.checkNotNull(nsObject.getName());
    Preconditions.checkNotNull(nsObject.getTable());
    if (nsObject instanceof TableSourceNamespaceObject) {
      TableSourceNamespaceObject tableSourceNamespaceObject = (TableSourceNamespaceObject) nsObject;
      return schema.addTable(name, tableSourceNamespaceObject.getSource());
    } else if (nsObject instanceof TableSourceSinkNamespaceObject) {
      TableSourceSinkNamespaceObject tableSourceNamespaceObject = (TableSourceSinkNamespaceObject) nsObject;
      return schema.addTable(name, tableSourceNamespaceObject.getSource());
    } else if (nsObject instanceof TableSinkNamespaceObject) {
      throw new RuntimeException("Cannot import a sink directly");
    } else if (nsObject instanceof SqrlTableNamespaceObject) {
      SqrlTableNamespaceObject sqrlTable = (SqrlTableNamespaceObject) nsObject;
      schema.registerScriptTable(sqrlTable.getTable());
      return true;
    } else {
      throw new RuntimeException("unknown");
    }
  }

  @Override
  public SqlOperatorTable getOperatorTable() {
    return getSchema().getFunctionCatalog().getOperatorTable();
  }

  @Override
  public SqrlSchema getSchema() {
    return schema;
  }

  @Override
  public List<ResolvedExport> getExports() {
    return exports;
  }

  @Override
  public void addExport(ResolvedExport resolvedExport) {
    exports.add(resolvedExport);
  }
}
