package ai.datasqrl.schema;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;

import ai.datasqrl.plan.local.BundleTableFactory;
import ai.datasqrl.schema.SourceTableImportMeta.RowType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;

public class Schema extends ShadowingContainer<Table> {

  //TODO: unpack this
  @Getter
  final BundleTableFactory tableFactory = new BundleTableFactory();

  public Map<Table, RowType> addImportTable(SourceTableImport importSource, Optional<Name> nameAlias) {
    Pair<Table, Map<Table, RowType>> resolvedImport = tableFactory.importTable(importSource, nameAlias);
    Table table = resolvedImport.getKey();
    add(table);
    return resolvedImport.getRight();
  }

  public Optional<Table> getTable(Name name) {
    return this.getVisibleByName(name);
  }

  public Table walkTable(NamePath tablePath) {
    if (tablePath.getLength() == 1) {
      return this.getVisibleByName(tablePath.getFirst()).get();
    } else {
      return this.getVisibleByName(tablePath.getFirst()).get()
          .walkTable(tablePath.popFirst()).get();
    }
  }

  public List<Table> allTables() {
    List<Table> tables = new ArrayList<>();
    for (Table t : this) addTableAndChildren(t,tables);
    return tables;
  }

  private void addTableAndChildren(Table t, List<Table> tables) {
    tables.add(t);
    for (Table child : t.getChildren()) {
      addTableAndChildren(child, tables);
    }
  }

  public String toString() {
    StringBuilder s = new StringBuilder();
    for (Table t : allTables()) {
      s.append(t).append("\n");
    }
    return s.toString();
  }
}
