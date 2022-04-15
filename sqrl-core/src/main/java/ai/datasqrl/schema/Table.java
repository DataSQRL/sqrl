package ai.datasqrl.schema;

import ai.datasqrl.schema.Relationship.Type;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.VersionedName;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.rel.RelNode;

@Getter
@Setter
public class Table implements ShadowingContainer.Nameable {

  public Name name;
  public final int uniqueId;
  public ShadowingContainer<Field> fields = new ShadowingContainer<>();
  private final NamePath path;
  public final boolean isInternal;
  private RelNode relNode;

  private List<Field> partitionKeys;

  public Table(int uniqueId, Name name, NamePath path, boolean isInternal) {
    this.name = name;
    this.uniqueId = uniqueId;
    this.path = path;
    this.isInternal = isInternal;
  }

  public Field getField(Name name) {
    Optional<Field> field = fields.getByName(name);
    return field.isEmpty() ? null : field.get();
  }

  public Field getField(VersionedName name) {
    for (Field field : this.fields.getElements()) {
      if (field.getId().equals(name)) {
        return field;
      }
    }
    return null;
  }

  public boolean addField(Field field) {
    return fields.add(field);
  }

  public VersionedName getId() {
    return VersionedName.of(name, uniqueId);
  }

  public List<Column> getPrimaryKeys() {
    List<Column> pks = this.fields.visibleStream()
        .filter(f->f instanceof Column)
        .map(f->(Column) f)
        .filter(f->f.isPrimaryKey)
        .collect(Collectors.toList());

    //Bad remove this
    if (pks.isEmpty()) {
      return List.of((Column)this.fields.get(0));
    }

    return pks;
  }

  public boolean isVisible() {
    return !isInternal;
  }

  public int getVersion() {
    return uniqueId;
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

  public Optional<Field> walkField(NamePath namePath) {
    if (namePath.isEmpty()) {
      return Optional.empty();
    }
    Field field = getField(namePath.getFirst());
    if (field == null) {
      return Optional.empty();
    }

    if (namePath.getLength() == 1) {
      return Optional.of(field);
    }

    if (field instanceof Relationship) {
      Relationship relationship = (Relationship) field;
      return relationship.getToTable()
          .walkField(namePath.popFirst());
    }

    return Optional.of(field);
  }

  public Optional<Table> walk(NamePath namePath) {
    if (namePath.isEmpty()) {
      return Optional.of(this);
    }
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

  public Optional<Table> getParent() {
    for (Field field : fields) {
      if (field instanceof Relationship && ((Relationship)field).getType() == Type.PARENT) {
        return Optional.of(((Relationship)field).getToTable());
      }
    }
    return Optional.empty();
  }

  @Override
  public String toString() {
    return "Table{" +
        "name=" + name +
//        ", pk=" + getPrimaryKeys().stream().map(e->e.getName()).collect(Collectors.toList()) +
//        ", fk=" + getForeignKeys().stream().map(e->e.getName()).collect(Collectors.toList()) +
//        ", fields=" + getFields().stream().map(e->e.getName()).collect(Collectors.toList()) +
//        ", relNode=" + getRelNode().explain() +
        '}';
  }

  public void setRelNode(RelNode relNode) {
    this.relNode = relNode;
  }

  public void addUniqueConstraint(List<Field> partitionKeys) {
    this.partitionKeys = partitionKeys;
  }

  /**
   * Creates a field but does not bind it to this table
   */
  public Column fieldFactory(Name name) {
    if (name instanceof VersionedName) {
      name = ((VersionedName)name).toName();
    }
    int version = 0;
    if (getField(name) != null) {
      version = getField(name).getVersion() + 1;
    }

    return new Column(name, this, version, null, 0, List.of(), false, false, Optional.empty(), false);
  }

  public Optional<Column> getEquivalent(Column lhsColumn) {
    for (Field field : this.getFields()) {
      if (field instanceof Column) {
        if (((Column)field).getSource() == lhsColumn.getSource()) {
          return Optional.of((Column) field);
        }
      }
    }

    return Optional.empty();
  }

  public List<Integer> getParentPrimaryKeys() {
    List<Integer> pos = new ArrayList<>();
    List<Field> elements = fields.getElements();
    for (int i = 0; i < elements.size(); i++) {
      Field field = elements.get(i);
      if (field instanceof Column && ((Column) field).getParentPrimaryKey()) {
        pos.add(i);
      }
    }
    return pos;
  }
}
