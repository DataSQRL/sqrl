package ai.datasqrl.plan;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

@Value
public class ImportTable {
  NamePath tableName;
  RelNode relNode;
  List<Name> fields;
}
