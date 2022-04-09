package ai.dataeng.sqml.parser.sqrl.analyzer;

import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.name.Name;
import lombok.Value;

@Value
public class TableBookkeeping {

  Relation current;
  Name alias;
  Table currentTable;
}
