package ai.dataeng.sqml.physical;

import ai.dataeng.sqml.ViewQueryRewriter.ViewTable;
import ai.dataeng.sqml.tree.QualifiedName;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Value;

@Value
public class PhysicalModel {
  private List<ViewTable> tables;

  public PhysicalModel() {
    this.tables = new ArrayList<>();
  }

  public void addTable(ViewTable viewTable) {
    this.tables.add(viewTable);
  }

  public Optional<ViewTable> getTableByName(QualifiedName name) {
    for (int i = tables.size() - 1; i >= 0; i--) {
      ViewTable table = tables.get(i);
      if (table.getPath().equalsCanonical(name)) {
        return Optional.of(table);
      }
    }

    return Optional.empty();
  }
}
