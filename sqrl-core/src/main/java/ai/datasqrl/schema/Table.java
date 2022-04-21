package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.VersionedName;
import ai.datasqrl.schema.Relationship.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;

@Getter
@Setter
public class Table implements ShadowingContainer.Nameable {

  public Name name;
  public final int uniqueId;
  public ShadowingContainer<Field> fields = new ShadowingContainer<>();
  private final NamePath path;
  public final boolean isInternal;
  private RelNode relNode;
  private Set<Integer> primaryKey;
  private Set<Integer> parentPrimaryKey;

  private List<Field> partitionKeys;

  public Table(int uniqueId, Name name, NamePath path, boolean isInternal) {
    this(uniqueId, name, path, isInternal, null, Set.of(), Set.of());
  }

  public Table(int uniqueId, Name name, NamePath path, boolean isInternal,
      RelNode relNode, Set<Integer> primaryKey, Set<Integer> parentPrimaryKey) {
    this.name = name;
    this.uniqueId = uniqueId;
    this.path = path;
    this.isInternal = isInternal;
    this.relNode = relNode;
    this.primaryKey = primaryKey;
    this.parentPrimaryKey = parentPrimaryKey;
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
    Set<Integer> pk = new HashSet<>();
    pk.addAll(this.primaryKey);
    pk.addAll(this.parentPrimaryKey);

    List<Column> pks = new ArrayList<>();
    for (Integer index : pk) {
      RelDataTypeField field = this.relNode.getRowType().getFieldList().get(index);
      pks.add((Column) this.fields.getByName(VersionedName.parse(field.getName()).toName()).get());
    }

    return pks;
  }
//
//  public List<Name> getPrimaryKeyNames() {
//    Set<Integer> pk = new HashSet<>();
//    pk.addAll(this.primaryKey);
//    pk.addAll(this.parentPrimaryKey);
//
//    List<Name> pks = new ArrayList<>();
//    for (Integer index : pk) {
//      RelDataTypeField field = this.relNode.getRowType().getFieldList().get(index);
//      pks.add(VersionedName.parse(field.getName()));
//    }
//
//    return pks;
//  }

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
      if (field instanceof Relationship && ((Relationship) field).getType() == Type.PARENT) {
        return Optional.of(((Relationship) field).getToTable());
      }
    }
    return Optional.empty();
  }

  @Override
  public String toString() {
    return "Table{" +
        "name=" + name +
        '}';
  }

  public void setRelNode(RelNode relNode) {
    this.relNode = relNode;
  }

  /**
   * Determines the next field name
   */
  public Name getNextFieldName(Name name) {
    if (name instanceof VersionedName) {
      name = ((VersionedName) name).toName();
    }
    int version = 0;
    if (getField(name) != null) {
      version = getField(name).getVersion() + 1;
    }

    return VersionedName.of(name, version);
  }

  public Optional<Column> getEquivalent(Column lhsColumn) {
    for (Field field : this.getFields()) {
      if (field instanceof Column) {
        if (((Column) field).getSource() == lhsColumn.getSource()) {
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
      if (field instanceof Column && ((Column) field).isParentPrimaryKey()) {
        pos.add(i);
      }
    }
    return pos;
  }

  //Todo: Validate first
  public List<Field> walkFields(NamePath names) {
    List<Field> fields = new ArrayList<>();
    Table current = this;
    Name[] namesNames = names.getNames();
    for (int i = 0; i < namesNames.length; i++) {
      Name field = namesNames[i];
      Field f = current.getField(field);
      if (f instanceof Relationship) {
        Relationship rel = (Relationship) current.getField(field);
        current = rel.getToTable();
      } else {
        current = null;
      }
      fields.add(f);
    }
    return fields;
  }
}
