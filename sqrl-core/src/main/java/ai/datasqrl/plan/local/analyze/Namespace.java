package ai.datasqrl.plan.local.analyze;

import ai.datasqrl.parse.tree.TableNode;
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

  public Optional<ResolvedTable> resolveTable(TableNode tableNode) {
    Table table = schema.walkTable(tableNode.getNamePath()); //todo

    ResolvedTable resolvedTable = new ResolvedTable(
        tableNode.getNamePath().getFirst().getCanonical(), Optional.empty(),
        List.of(new RootTableField(table)));
    return Optional.of(resolvedTable);
  }
}
