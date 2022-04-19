package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import org.apache.calcite.rel.RelNode;

public class TableFactory2 {


  /**
   * TODO: move to table factory
   */
  public Table create(Name name, NamePath namePath, RelNode relNode,
      List<Name> fields) {
    Table table = new Table(SourceTablePlanner.tableIdCounter.incrementAndGet(),
        name, namePath,
        false, relNode);
    for (Name n : fields) {
      table.addField(Column.createTemp(n, null, table, 0));
    }

    return table;
  }
}
