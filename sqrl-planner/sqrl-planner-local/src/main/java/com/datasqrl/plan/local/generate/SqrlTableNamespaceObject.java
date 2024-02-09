package com.datasqrl.plan.local.generate;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.local.ScriptTableDefinition;
import com.datasqrl.plan.table.CalciteTableFactory;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqrlTableFunctionDef;

@Getter
public class SqrlTableNamespaceObject extends AbstractTableNamespaceObject<ScriptTableDefinition> {
  private final Name name;
  private final ScriptTableDefinition table;
  private final SqrlTableFunctionDef args;
  private final List<FunctionParameter> parameters;
  private final List<Function> isA;
  private final boolean materializeSelf;
  private final List<FunctionParameter> functionParameters;
  private final Optional<Supplier<RelNode>> relNodeSupplier;

  public SqrlTableNamespaceObject(Name name, ScriptTableDefinition table, CalciteTableFactory tableFactory,
      SqrlTableFunctionDef args,
      List<FunctionParameter> parameters, List<Function> isA, boolean materializeSelf,
      List<FunctionParameter> functionParameters, Optional<Supplier<RelNode>> relNodeSupplier) {
    super(tableFactory, NameCanonicalizer.SYSTEM);
    this.name = name;
    this.table = table;
    this.args = args;
    this.parameters = parameters;
    this.isA = isA;
    this.materializeSelf = materializeSelf;
    this.functionParameters = functionParameters;
    this.relNodeSupplier = relNodeSupplier;
  }

  @Override
  public boolean apply(Optional<String> objectName, SqrlFramework framework, ErrorCollector errors) {
    registerScriptTable(table, framework, Optional.of(functionParameters),
        relNodeSupplier);

    return true;
  }
}
