package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import java.util.Set;
import org.apache.calcite.rel.RelNode;

public class TableFactory2 {


  /**
   * TODO: move to table factory
   */
  public Table create(Name name, NamePath namePath, RelNode relNode,
      List<Name> fields, Set<Integer> primaryKey,
      Set<Integer> parentPrimaryKey, Table parentTable) {
    Table table = new Table(SourceTablePlanner.tableIdCounter.incrementAndGet(),
        name, namePath,
        false, relNode, primaryKey, parentPrimaryKey);
    for (Name n : fields) {
      table.addField(Column.createTemp(n, null, table, 0));
    }

    //Assign parent primary key source on columns for equivalence testing
    for (Integer i : parentPrimaryKey) {
      String parentColumn = relNode.getRowType().getFieldList().get(i).getName();
      Column thisColumn = (Column)table.getField(Name.system(parentColumn));
      thisColumn.setParentPrimaryKey(true);
      Column column = (Column)parentTable.getField(Name.system(parentColumn));
      column.setSource(thisColumn);
    }

    for (Integer i : primaryKey) {
      String parentColumn = relNode.getRowType().getFieldList().get(i).getName();
      Column thisColumn = (Column)table.getField(Name.system(parentColumn));
      thisColumn.setPrimaryKey(true);
    }

    return table;
  }
}
