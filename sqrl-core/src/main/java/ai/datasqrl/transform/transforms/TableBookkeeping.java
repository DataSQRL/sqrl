package ai.datasqrl.transform.transforms;

import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.Table;
import lombok.Value;

@Value
public class TableBookkeeping {

  Relation current;
  Name alias;
  Table currentTable;
}
