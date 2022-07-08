package ai.datasqrl.plan.local.analyzer;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.local.BundleTableFactory;
import ai.datasqrl.plan.local.operations.SourceTableImportOp.RowType;
import ai.datasqrl.schema.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;

public class LocalSchemaBuilder {
  final List<Table> schema = new ArrayList<>();

  //TODO: unpack this
  final BundleTableFactory tableFactory = new BundleTableFactory();

  public Map<Table, RowType> addImportTable(SourceTableImport importSource, Optional<Name> nameAlias) {
    Pair<Table, Map<Table, RowType>> resolvedImport = tableFactory.importTable(importSource, nameAlias);
    Table table = resolvedImport.getKey();
    schema.add(table);
    return resolvedImport.getRight();
  }

  public Optional<Table> getTable(Name name) {
    for (Table table : schema) {
      if (table.getName().equals(name)) {
        return Optional.of(table);
      }
    }
    return Optional.empty();
  }
}
