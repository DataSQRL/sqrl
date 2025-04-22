package com.datasqrl.plan.local.generate;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqrlTableFunctionDef;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.plan.validate.ScriptPlanner;

import lombok.Getter;

@Getter
public class SqrlTableNamespaceObject extends AbstractTableNamespaceObject<PhysicalRelationalTable> {
  private final Name name;
  private final PhysicalRelationalTable table;
  private final SqrlTableFunctionDef args;
  private final List<FunctionParameter> parameters;
  private final List<Function> isA;
  private final boolean materializeSelf;
  private final List<FunctionParameter> functionParameters;
  private final Optional<Supplier<RelNode>> relNodeSupplier;
  private final boolean isTest;
  private final Optional<SqlNodeList> opHints;

  public SqrlTableNamespaceObject(Name name, PhysicalRelationalTable table, CalciteTableFactory tableFactory,
      SqrlTableFunctionDef args,
      List<FunctionParameter> parameters, List<Function> isA, boolean materializeSelf,
      List<FunctionParameter> functionParameters, Optional<Supplier<RelNode>> relNodeSupplier,
      ModuleLoader moduleLoader, boolean isTest, Optional<SqlNodeList> opHints) {
    super(tableFactory, NameCanonicalizer.SYSTEM, moduleLoader);
    this.name = name;
    this.table = table;
    this.args = args;
    this.parameters = parameters;
    this.isA = isA;
    this.materializeSelf = materializeSelf;
    this.functionParameters = functionParameters;
    this.relNodeSupplier = relNodeSupplier;
    this.isTest = isTest;
    this.opHints = opHints;
  }

  @Override
  public boolean apply(ScriptPlanner planner, Optional<String> objectName, SqrlFramework framework, ErrorCollector errors) {
    registerScriptTable(table, framework, Optional.of(functionParameters),
        relNodeSupplier, isTest, false, opHints.flatMap(this::execHint));

    return true;
  }

  private Optional<Boolean> execHint(SqlNodeList hintList) {
    return hintList.getList()
        .stream().map(e->(SqlHint)e)
        .filter(e->e.getName().equalsIgnoreCase("exec"))
        .map(e->Boolean.TRUE)
        .findAny();
  }
}
