package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.analyzer.Analysis.ResolvedNamePath;
import java.util.Optional;
import lombok.Getter;

public class RootTableField extends Field {

  @Getter
  private final Table table;

  //Todo: migrate to versioned table
  public RootTableField(Table table) {
    super(table.getName());
    this.table = table;
  }

  @Override
  public Name getId() {
    return table.getId();
  }

  @Override
  public int getVersion() {
    return table.getVersion();
  }

  public Optional<ResolvedNamePath> walk(NamePath namePath) {
    return null;
  }
}