package ai.datasqrl.plan.local.analyze;

import ai.datasqrl.function.FunctionMetadataProvider;
import ai.datasqrl.function.SqrlAwareFunction;
import ai.datasqrl.function.calcite.CalciteFunctionMetadataProvider;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedTable;
import ai.datasqrl.schema.DatasetTable;
import ai.datasqrl.schema.RootTableField;
import ai.datasqrl.schema.VarTable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Namespace {

  /**
   * Allow resolution of dataset name: ecommerce-data.Orders
   */
  Map<Name, DatasetTable> scopedDatasets = new HashMap<>();

  /**
   * Allow resolution of tables by name: Orders
   */
  Map<Name, VarTable> scopedTables = new HashMap<>();

  private static final FunctionMetadataProvider functionMetadataProvider =
      new CalciteFunctionMetadataProvider(
          SqrlOperatorTable.instance());

  public Optional<ResolvedTable> resolveTable(TableNode tableNode) {
    Optional<VarTable> tableOpt = getTable(tableNode.getNamePath());
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

  public void scopeDataset(DatasetTable datasetTable, Optional<Name> nameAlias) {
    scopedDatasets.put(nameAlias.orElse(datasetTable.getName()), datasetTable);

    scopedTables.put(datasetTable.getTable().getName(), datasetTable.getTable());
  }

  public void addTable(VarTable varTable) {
    scopedTables.put(varTable.getName(), varTable);
  }

  public Optional<VarTable> getTable(Name name) {
    return Optional.ofNullable(scopedTables.get(name));
  }

  public Optional<VarTable> getTable(NamePath namePath) {
    Name first = namePath.getFirst();
    if (scopedDatasets.get(first) != null) {
      return scopedDatasets.get(first).walkTable(namePath);
    }
    Optional<VarTable> table = Optional.ofNullable(scopedTables.get(first));
    if (namePath.popFirst().size() == 0) {
      return table;
    }
    return table.flatMap(t->t.walkTable(namePath.popFirst()));
  }
}
