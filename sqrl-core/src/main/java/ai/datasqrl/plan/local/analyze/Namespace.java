package ai.datasqrl.plan.local.analyze;

import ai.datasqrl.function.FunctionMetadataProvider;
import ai.datasqrl.function.SqrlAwareFunction;
import ai.datasqrl.function.calcite.CalciteFunctionMetadataProvider;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedNamePath;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedTable;
import ai.datasqrl.schema.RootTableField;
import ai.datasqrl.schema.Schema;
import ai.datasqrl.schema.Table;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class Namespace {
  Schema schema;

  private static final FunctionMetadataProvider functionMetadataProvider =
      new CalciteFunctionMetadataProvider(
          SqrlOperatorTable.instance());

  public Optional<ResolvedTable> resolveTable(TableNode tableNode) {
    Table table = schema.walkTable(tableNode.getNamePath()); //todo

    ResolvedTable resolvedTable = new ResolvedTable(
        tableNode.getNamePath().getFirst().getCanonical(), Optional.empty(),
        List.of(new RootTableField(table)));
    return Optional.of(resolvedTable);
  }

  public Optional<SqrlAwareFunction> lookupFunction(NamePath namePath) {
    return functionMetadataProvider.lookup(namePath);
  }
}
