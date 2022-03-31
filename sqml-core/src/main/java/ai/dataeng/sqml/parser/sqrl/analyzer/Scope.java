package ai.dataeng.sqml.parser.sqrl.analyzer;

import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.FieldPath;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Builder
@Getter
@Setter
public class Scope {
  private Optional<Table> contextTable;
  private Node node;

  //Alias mapping of tables
  private Map<Name, Table> joinScope;

  public List<Field> resolveFieldsWithPrefix(Optional<Name> alias) {
    if (alias.isPresent()) {
      return joinScope.get(alias.get()).getFields().getElements();
    }

    List<Field> allFields = new ArrayList<>();
    for (Table table : joinScope.values()) {
      allFields.addAll(table.getFields().getElements());
    }

    return allFields;
  }

  public Map<Name, Table> getJoinScope() {
    return joinScope;
  }

  public List<FieldPath> resolve(NamePath namePath) {
    List<FieldPath> fieldPaths = new ArrayList<>();
    for (Map.Entry<Name, Table> entry : joinScope.entrySet()) {
      Name alias = entry.getKey();
      Table table = entry.getValue();

      if (namePath.getFirst().equals(alias) && namePath.getLength() > 1) {
        Optional<FieldPath> path = table.getField(namePath.popFirst());
        path.ifPresent(fieldPaths::add);
      }
      Optional<FieldPath> path = table.getField(namePath);
      path.ifPresent(fieldPaths::add);
    }

    return fieldPaths;
  }
}
