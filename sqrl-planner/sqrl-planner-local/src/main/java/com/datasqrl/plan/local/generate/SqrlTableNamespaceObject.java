package com.datasqrl.plan.local.generate;

import static com.datasqrl.plan.table.CalciteTableFactory.createPkWrapper;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.plan.local.ScriptTableDefinition;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.table.VirtualRelationalTable;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.Relationship.JoinType;
import com.datasqrl.schema.SQRLTable;
import java.util.List;
import java.util.Map.Entry;
import lombok.Getter;

import java.util.Optional;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqrlTableFunctionDef;
import org.apache.commons.lang3.tuple.Pair;

@Getter
public class SqrlTableNamespaceObject extends AbstractTableNamespaceObject<ScriptTableDefinition> {
  private final Name name;
  private final ScriptTableDefinition table;
  private final SqrlTableFunctionDef args;
  private final List<FunctionParameter> parameters;
  private final List<Function> isA;
  private final boolean materializeSelf;

  public SqrlTableNamespaceObject(Name name, ScriptTableDefinition table, CalciteTableFactory tableFactory,
      SqrlTableFunctionDef args,
      List<FunctionParameter> parameters, List<Function> isA, boolean materializeSelf) {
    super(tableFactory);
    this.name = name;
    this.table = table;
    this.args = args;
    this.parameters = parameters;
    this.isA = isA;
    this.materializeSelf = materializeSelf;
  }

  @Override
  public boolean apply(Optional<String> objectName, SqrlFramework framework, ErrorCollector errors) {
    /**
     * We have the sqrl tables now, implicit joins are completed.
     * We need to add it to the schema now.
     */
    registerScriptTable(table, framework);

    if (materializeSelf) {
//      Entry<SQRLTable, VirtualRelationalTable> table1 = table.getShredTableMap().entrySet().stream()
//          .filter(f -> f.getValue().isRoot())
//          .findAny().get();
//
//      Pair<List<FunctionParameter>, SqlNode> pkWrapper = createPkWrapper(table1.getValue(), table1.getValue());
//
//      Relationship rel = new Relationship(name, path, framework.getUniqueColumnInt().incrementAndGet(),
//          table1.getKey(), JoinType.CHILD, Multiplicity.MANY, List.of(table1.getKey()), pkWrapper.getLeft(),
//          ()->framework.getQueryPlanner().plan(Dialect.CALCITE, pkWrapper.getRight()));
//
//      framework.getSchema().plus().add(rel.getPath().toString() + "$" + framework.getUniqueMacroInt().incrementAndGet(),
//          rel);
    }

    return true;
  }
}
