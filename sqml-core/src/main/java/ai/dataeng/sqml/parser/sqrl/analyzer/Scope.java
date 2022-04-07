package ai.dataeng.sqml.parser.sqrl.analyzer;

import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.FieldPath;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.tree.Join;
import ai.dataeng.sqml.tree.JoinCriteria;
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

  public List<FieldPath> resolveField(NamePath namePath) {
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

    //Special case for self identifier
    // If we haven't scoped '_' then treat it as the context table.
    if (fieldPaths.isEmpty() && namePath.getFirst().equals(Name.SELF_IDENTIFIER)
      && joinScope.get(Name.SELF_IDENTIFIER) == null) {
      Optional<FieldPath> path = contextTable.get().getField(namePath.popFirst());
      path.ifPresent(fieldPaths::add);
    }

    return fieldPaths;
  }

  public NamePath qualify(NamePath namePath) {
    for (Map.Entry<Name, Table> entry : joinScope.entrySet()) {
      Name alias = entry.getKey();
      Table table = entry.getValue();

      if (namePath.getFirst().equals(alias) && namePath.getLength() > 1) {
        Optional<FieldPath> path = table.getField(namePath.popFirst());
        if (path.isPresent()) {
          return alias.toNamePath().concat(path.get().qualify());
        }
      }
      Optional<FieldPath> path = table.getField(namePath);
      if (path.isPresent()) {
        return NamePath.of(alias).concat(path.get().qualify());
      }
    }

    throw new RuntimeException("Cannot qualify");
  }

  public Optional<JoinCriteria> getAdditionalJoinCondition() {
    return Optional.empty();
  }
}
