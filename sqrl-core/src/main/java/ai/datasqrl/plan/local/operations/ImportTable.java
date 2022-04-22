package ai.datasqrl.plan.local.operations;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import java.util.Set;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

@Value
public class ImportTable {

  NamePath tableName;
  RelNode relNode;
  List<Name> fields;
  Set<Integer> primaryKey;
  Set<Integer> parentPrimaryKey;
}
