package ai.dataeng.sqml.analyzer2;

import ai.dataeng.sqml.tree.name.Name;
import java.util.HashMap;
import java.util.Map;
import lombok.Value;

@Value
public class TableManager {
  Map<Name, SqrlEntity> tables = new HashMap<>();
}
