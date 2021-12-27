package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.planner.LogicalPlanImpl.RowNode;
import ai.dataeng.sqml.planner.operator.ShadowingContainer;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.Objects;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
public class Table implements DatasetOrTable {

  public Name name;
  public final int uniqueId;
  public final ShadowingContainer<Field> fields = new ShadowingContainer<>();
  public final boolean isInternal;
  public RowNode currentNode;


  public Table(int uniqueId, Name name, boolean isInternal) {
    this.name = name;
    this.uniqueId = uniqueId;
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
      relationship.getToTable()
          .walk(namePath.popFirst());
    }

    return Optional.empty();
  }

}
