package ai.dataeng.sqml.analyzer2;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.HashMap;
import java.util.Map;
import lombok.Value;

@Value
public class TableManager {
  Map<NamePath, SqrlEntity> tables = new HashMap<>();
}
