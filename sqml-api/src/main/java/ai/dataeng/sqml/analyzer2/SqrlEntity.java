package ai.dataeng.sqml.analyzer2;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.table.api.Table;

@Getter
@Setter
public class SqrlEntity {

  private final NamePath namePath;
  Table table;
  SqrlEntity parent = null;
  Map<Name, SqrlEntity> relationships = new HashMap<>();
  List<Name> primaryKey = new ArrayList<>();
  public SqrlEntity(NamePath namePath, Table table) {
    this.namePath = namePath;
    this.table = table;
  }

  public void addRelationship(Name name, SqrlEntity entity) {
    relationships.put(name, entity);
    entity.setParent(this);
  }

  public List<Name> getPrimaryKey() {
    return primaryKey;
  }

  public List<Name> getContextKey() {
    List<Name> contextKeys = new ArrayList<>();
    if (parent != null) {
      contextKeys.addAll(parent.getContextKey());
    }
    contextKeys.addAll(primaryKey);
    return contextKeys;
  }
}
