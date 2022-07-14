package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.schema.Relationship.JoinType;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;

@Getter
public class Table extends AbstractTable {
  public Table(int uniqueId, NamePath path, ShadowingContainer<Field> fields) {
    super(uniqueId,path,fields);
  }

  public void addExpressionColumn(Column column) {
    fields.add(column);
  }

  public Optional<Table> walkTable(NamePath namePath) {
    if (namePath.isEmpty()) {
      return Optional.of(this);
    }
    Optional<Field> field = getField(namePath.getFirst());
    if (field.isEmpty()) {
      return Optional.empty();
    }

    if (namePath.getLength() == 1) {
      if (field.get() instanceof Relationship) {
        return Optional.of(((Relationship) field.get()).getToTable());
      } else {
        return Optional.empty();
      }
    }

    if (field.get() instanceof Relationship) {
      Relationship relationship = (Relationship) field.get();
      return relationship.getToTable()
          .walkTable(namePath.popFirst());
    }

    return Optional.empty();
  }

  public Optional<Table> getParent() {
    return getAllRelationships().filter(r -> r.getJoinType() == JoinType.PARENT).map(Relationship::getToTable).findFirst();
  }

  public Collection<Table> getChildren() {
    return getAllRelationships().filter(r -> r.getJoinType() == JoinType.CHILD).map(Relationship::getToTable).collect(Collectors.toList());
  }

  public List<Column> getVisibleColumns() {
    return getAllColumns().filter(Column::isVisible).collect(Collectors.toList());
  }

  public void addColumn(Name name, boolean primaryKey, boolean parentPrimaryKey, boolean visible) {
    int version = this.fields.getMaxVersion(name).map(v->v+1).orElse(0);

    this.fields.add(new Column(name, version, fields.size(), primaryKey, parentPrimaryKey, visible));
  }

  public NamePath getPath() {
    if (getField(ReservedName.PARENT).isPresent()) {
      return ((Relationship) getField(ReservedName.PARENT).get())
          .getToTable().getPath().concat(getName());
    } else {
      return getName().toNamePath();
    }
  }
}
