package ai.datasqrl.transform;

import ai.datasqrl.schema.Table;
import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.parse.tree.name.Name;
import lombok.Value;

@Value
public class TableBookkeeping {

  Relation current;
  Name alias;
  Table currentTable;
}
