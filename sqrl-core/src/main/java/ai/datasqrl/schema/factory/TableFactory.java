package ai.datasqrl.schema.factory;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.schema.Relationship.Type;
import ai.datasqrl.schema.Schema;
import ai.datasqrl.schema.Table;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.rel.RelNode;

public class TableFactory {

  public final static AtomicInteger tableIdCounter = new AtomicInteger(0);

  public void createTable(Schema schema, NamePath tableName, List<Name> fields, RelNode relNode,
      Set<Integer> primaryKey, Set<Integer> parentPrimaryKey) {
    NamePath namePath = tableName;
    if (namePath.getLength() == 1) {
      Table table = create(namePath.getFirst(), namePath, relNode,
          fields,
          primaryKey, parentPrimaryKey, null);
      schema.add(table);
    } else {
      Table table = schema.getByName(namePath.get(0)).get();
      Name[] names = namePath.getNames();
      for (int i = 1; i < names.length - 1; i++) {
        Name name = names[i];
        Field field = table.getField(name);
        if (names.length - 2 != i) {
          table = ((Relationship) field).getToTable();
        }
      }
      Table newTable = create(namePath.getLast(), namePath, relNode, fields,
          primaryKey, parentPrimaryKey, table);
      Relationship relationship = new Relationship(namePath.getLast(),
          table, newTable, Type.CHILD, Multiplicity.MANY);
      table.addField(relationship);
    }
  }

  private Table create(Name name, NamePath namePath, RelNode relNode,
      List<Name> fields, Set<Integer> primaryKey,
      Set<Integer> parentPrimaryKey, Table parentTable) {
    Table table = new Table(tableIdCounter.incrementAndGet(),
        name, namePath,
        false, relNode, primaryKey, parentPrimaryKey);
    for (Name n : fields) {
      table.addField(Column.createTemp(n, null, table, 0));
    }

    //Assign parent primary key source on columns for equivalence testing
    for (Integer i : parentPrimaryKey) {
      String parentColumn = relNode.getRowType().getFieldList().get(i).getName();
      Column thisColumn = (Column) table.getField(Name.system(parentColumn));
      thisColumn.setParentPrimaryKey(true);
      Column column = (Column) parentTable.getField(Name.system(parentColumn));
      column.setSource(thisColumn);
    }

    for (Integer i : primaryKey) {
      String parentColumn = relNode.getRowType().getFieldList().get(i).getName();
      Column thisColumn = (Column) table.getField(Name.system(parentColumn));
      thisColumn.setPrimaryKey(true);
    }

    return table;
  }
}
