package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.planner.LogicalPlanImpl.RowNode;
import ai.dataeng.sqml.planner.operator.ShadowingContainer;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Table implements DatasetOrTable {

  public Name name;
  public final int uniqueId;
  public final ShadowingContainer<Field> fields = new ShadowingContainer<>();
  private final NamePath path;
  public final boolean isInternal;
  public RowNode currentNode;

  public Table(int uniqueId, Name name, NamePath path, boolean isInternal) {
    this.name = name;
    this.uniqueId = uniqueId;
    this.path = path;
    this.isInternal = isInternal;
  }

  public void updateNode(RowNode node) {
    currentNode = node;
  }

  public Field getField(Name name) {
    return fields.getByName(name);
  }

  public boolean addField(Field field) {
    return fields.add(field);
  }

  public String getId() {
    return name.getCanonical() + LogicalPlanImpl.ID_DELIMITER + Integer.toHexString(uniqueId);
  }

  public List<Column> getPrimaryKeys() {
    return this.fields.visibleStream()
        .filter(f->f instanceof Column)
        .map(f->(Column) f)
        .filter(f->f.isPrimaryKey)
        .collect(Collectors.toList());
  }

  @Override
  public boolean isVisible() {
    return !isInternal;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Table table = (Table) o;
    return uniqueId == table.uniqueId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(uniqueId);
  }

  public Optional<Table> walk(NamePath namePath) {
    Field field = getField(namePath.getFirst());
    if (field == null) {
      return Optional.empty();
    }

    if (namePath.getLength() == 1) {
      if (field instanceof Relationship) {
        return Optional.of(((Relationship) field).getToTable());
      } else {
        return Optional.empty();
      }
    }

    if (field instanceof Relationship) {
      Relationship relationship = (Relationship) field;
      return relationship.getToTable()
          .walk(namePath.popFirst());
    }

    return Optional.empty();
  }
}
