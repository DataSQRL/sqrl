package ai.datasqrl.plan.local.analyze;

import ai.datasqrl.function.FunctionMetadataProvider;
import ai.datasqrl.function.SqrlAwareFunction;
import ai.datasqrl.function.calcite.CalciteFunctionMetadataProvider;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.local.ScriptTableDefinition;
import ai.datasqrl.plan.local.RootTableField;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedTable;
import ai.datasqrl.schema.ScriptTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Namespace {

  /**
   * Allow resolution of tables by name: Orders
   */
  Map<Name, ScriptTable> scopedTables = new HashMap<>();

  private static final FunctionMetadataProvider functionMetadataProvider =
      new CalciteFunctionMetadataProvider(
          SqrlOperatorTable.instance());

  public Optional<ResolvedTable> resolveTable(TableNode tableNode) {
    Optional<ScriptTable> tableOpt = getTable(tableNode.getNamePath());
    if (tableOpt.isEmpty()) {
      return Optional.empty();
    }

    ResolvedTable resolvedTable = new ResolvedTable(
        tableNode.getNamePath().getFirst().getCanonical(), Optional.empty(),
        List.of(new RootTableField(tableOpt.get())));
    return Optional.of(resolvedTable);
  }

  public Optional<SqrlAwareFunction> lookupFunction(NamePath namePath) {
    return functionMetadataProvider.lookup(namePath);
  }

  public void scopeDataset(ScriptTableDefinition tableDefinition) {
    ScriptTable rootTable = tableDefinition.getTable();
    scopedTables.put(rootTable.getName(), rootTable);
  }

  public void addTable(ScriptTable table) {
    scopedTables.put(table.getName(), table);
  }

  public Optional<ScriptTable> getTable(Name name) {
    return Optional.ofNullable(scopedTables.get(name));
  }

  public Optional<ScriptTable> getTable(NamePath namePath) {
    Name first = namePath.getFirst();
    Optional<ScriptTable> table = Optional.ofNullable(scopedTables.get(first));
    if (namePath.popFirst().size() == 0) {
      return table;
    }
    return table.flatMap(t->t.walkTable(namePath.popFirst()));
  }
}
