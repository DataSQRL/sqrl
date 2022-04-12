package ai.dataeng.sqml.parser.sqrl.analyzer;

import ai.dataeng.sqml.parser.Column;
import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.Value;

@AllArgsConstructor
@Builder
@Getter
@Setter
public class Scope {
  private Optional<Table> contextTable;
  private Node node;

  //Alias mapping of tables
  private Map<Name, Table> joinScope;

  private boolean expression;
  private Name expressionName;

  public List<Identifier> resolveFieldsWithPrefix(Optional<Name> alias) {
    if (alias.isPresent()) {
      Table table = joinScope.get(alias.get());
      if (table == null) {
        System.out.println();
      }
      Preconditions.checkNotNull(table, "Could not find table %s", alias.get());
      List<Identifier> identifiers = joinScope.get(alias.get()).getFields().getElements().stream()
          .filter(f->f instanceof Column)
          .map(f->new Identifier(Optional.empty(), alias.get().toNamePath().concat(f.getName())))
          .collect(Collectors.toList());
      return identifiers;
    }

    List<Identifier> allFields = new ArrayList<>();
    for (Map.Entry<Name, Table> entry : joinScope.entrySet()) {
      List<Identifier> identifiers = entry.getValue().getFields().getElements().stream()
          .filter(f->f instanceof Column)
          .map(f->new Identifier(Optional.empty(), entry.getKey().toNamePath().concat(f.getName())))
          .collect(Collectors.toList());
      allFields.addAll(identifiers);
    }

    return allFields;
  }

  public Map<Name, Table> getJoinScope() {
    return joinScope;
  }

  public NamePath qualify(NamePath namePath) {
    for (Map.Entry<Name, Table> entry : joinScope.entrySet()) {
      Name alias = entry.getKey();
      Table table = entry.getValue();

      if (namePath.getFirst().equals(alias) && namePath.getLength() > 1) {
        Optional<Field> path = table.walkField(namePath.popFirst());
        if (path.isPresent()) {
          return alias.toNamePath().concat(path.get().getId());
        }
      }
      Optional<Field> path = table.walkField(namePath);
      if (path.isPresent()) {
        return NamePath.of(alias).concat(path.get().getId());
      }
    }

    throw new RuntimeException("Cannot qualify");
  }

  public Optional<Table> getJoinScope(Name name) {
    return Optional.ofNullable(joinScope.get(name));
  }

  public List<ResolveResult> resolveFirst(NamePath namePath) {
    List<ResolveResult> fields = new ArrayList<>();
    for (Map.Entry<Name, Table> entry : joinScope.entrySet()) {
      Name alias = entry.getKey();
      Table table = entry.getValue();

      if (namePath.getFirst().equals(alias) && namePath.getLength() > 1) {
        Field field = table.getField(namePath.get(1));
        if (field != null) {
          fields.add(new ResolveResult(field, namePath.subList(1, namePath.getLength()), alias, table));
        }
      }
      Field field = table.getField(namePath.getFirst());
      if (field != null) {
        fields.add(new ResolveResult(field, namePath.subList(0, namePath.getLength()), alias, table));
      }
    }

    return fields;
  }

  @Value
  public static class ResolveResult {
    Field firstField;
    Optional<NamePath> remaining;
    Name alias;
    Table table;
  }
}
