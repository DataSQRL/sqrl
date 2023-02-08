package com.datasqrl.plan.local.generate;

import com.datasqrl.error.ErrorCode;
import com.datasqrl.loaders.TableSourceNamespaceObject;
import com.datasqrl.name.Name;
import com.datasqrl.plan.calcite.TypeFactory;
import com.datasqrl.plan.calcite.table.AbstractRelationalTable;
import com.datasqrl.plan.calcite.table.CalciteTableFactory;
import com.datasqrl.plan.calcite.table.ProxySourceRelationalTable;
import com.datasqrl.plan.calcite.table.VirtualRelationalTable;
import com.datasqrl.plan.calcite.util.ContinuousIndexMap.Builder;
import com.datasqrl.plan.local.ScriptTableDefinition;
import com.datasqrl.schema.SQRLTable;
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
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.internal.FlinkEnvProxy;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.functions.UserDefinedFunction;


public class FlinkNamespace {

  private final SqrlCalciteSchema schema;
  //Temporary env for catalog construction, we'll remake this later
  TableEnvironmentImpl tempEnv = TableEnvironmentImpl.create(
      EnvironmentSettings.inStreamingMode().getConfiguration());

  Map<String, UserDefinedFunction> udfs = new HashMap<>();

  CalciteTableFactory tableFactory = new CalciteTableFactory(TypeFactory.getTypeFactory());
  Session session;
  @Getter
  private Set<URL> jars;

  public FlinkNamespace(Session session) {
    this.session = session;
    this.schema = session.getSchema();
    this.jars = new HashSet<>();
  }

  /**
   * Adds the given {@link NamespaceObject} to the namespace.
   *
   * @param nsObject the {@link NamespaceObject} to add
   * @return {@code true} if the object was successfully added; {@code false} otherwise.
   */
  public boolean addNsObject(NamespaceObject nsObject) {
    return addNsObject(nsObject.getName(), nsObject);
  }

  public boolean addNsObject(Name name, NamespaceObject nsObject) {
    if (nsObject instanceof FunctionNamespaceObject) {
      return addFunctionObject(name, (FunctionNamespaceObject) nsObject);
    } else if (nsObject instanceof TableNamespaceObject) {
      return addTableObject(name, (TableNamespaceObject) nsObject);
    } else {
      throw new RuntimeException("");
    }
  }

  /**
   * Adds the given {@link FunctionNamespaceObject} to the namespace.
   *
   * @param name
   * @param nsObject the {@link FunctionNamespaceObject} to add
   * @return {@code true} if the object was successfully added; {@code false} otherwise.
   */
  private boolean addFunctionObject(Name name, FunctionNamespaceObject<UserDefinedFunction> nsObject) {
    udfs.put(name.getCanonical(), nsObject.getFunction());

    tempEnv.createTemporarySystemFunction(name.getCanonical(), nsObject.getFunction());
    nsObject.getJarUrl().map(j->jars.add(j));
    return true;
  }

  /**
   * Adds the given {@link TableNamespaceObject} to the namespace.
   *
   * @param name
   * @param nsObject the {@link TableNamespaceObject} to add
   * @return {@code true} if the object was successfully added; {@code false} otherwise.
   */
  private boolean addTableObject(Name name, TableNamespaceObject nsObject) {
    Preconditions.checkNotNull(nsObject.getName());
    Preconditions.checkNotNull(nsObject.getTable());
    if (nsObject instanceof TableSourceNamespaceObject) {

      ScriptTableDefinition def = tableFactory.importTable(((TableSourceNamespaceObject) nsObject).getTable(),
          Optional.of(name),//todo can remove optional
          createRelBuilder(), session.getPipeline());

    session.getErrors().checkFatal(
        session.getSchema().getTable(def.getTable().getName().getCanonical(), false) == null,
        ErrorCode.IMPORT_NAMESPACE_CONFLICT,
        String.format("An item named `%s` is already in scope",
            def.getTable().getName().getDisplay()));
      registerScriptTable(def);
      return true;

    } else if (nsObject.getTable() instanceof ScriptTableDefinition) {
      registerScriptTable((ScriptTableDefinition) nsObject.getTable());
      return true;

//      schema.add(nsObject.getName().getCanonical(), nsObject.getTable());
    } else {
      throw new RuntimeException("unknown");
    }
  }

  public void registerScriptTable(ScriptTableDefinition tblDef) {
    for (Map.Entry<SQRLTable, VirtualRelationalTable> entry : tblDef.getShredTableMap()
        .entrySet()) {
      entry.getKey().setVT(entry.getValue());
      entry.getValue().setSqrlTable(entry.getKey());
    }
    schema.add(tblDef.getBaseTable().getNameId(), tblDef.getBaseTable());

    tblDef.getShredTableMap().values().stream().forEach(vt ->
        schema.add(vt.getNameId(),
            vt));

    if (tblDef.getBaseTable() instanceof ProxySourceRelationalTable) {
      AbstractRelationalTable impTable = ((ProxySourceRelationalTable) tblDef.getBaseTable()).getBaseTable();
      schema.add(impTable.getNameId(), impTable);
    }

    if (tblDef.getTable().getPath().size() == 1) {
      this.schema.add(tblDef.getTable().getName().getDisplay(), (org.apache.calcite.schema.Table)
          tblDef.getTable());
    }
  }
  public SqlOperatorTable getOperatorTable() {
    return FlinkEnvProxy.getOperatorTable(tempEnv);
  }

  public SqrlCalciteSchema getSchema() {
    return schema;
  }

  public RelBuilder createRelBuilder() {
    return session.createRelBuilder();
  }

  private List<ResolvedExport> exports = new ArrayList<>();
  public List<ResolvedExport> getExports() {
    return exports;
  }

  public void addExport(ResolvedExport resolvedExport) {
    exports.add(resolvedExport);
  }
}
