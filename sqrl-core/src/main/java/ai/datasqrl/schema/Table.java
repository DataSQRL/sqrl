package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.nodes.SqrlCalciteTable;
import ai.datasqrl.schema.Relationship.JoinType;
import ai.datasqrl.schema.attributes.ForeignKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

@Getter
public class Table implements ShadowingContainer.Nameable {
  public static final String ID_DELIMITER = "$";

  private final int uniqueId;
  private final NamePath path;
  private final boolean isInternal;
  private final ShadowingContainer<Field> fields;

  @Setter
  private RelNode head;

  public Table(int uniqueId, NamePath path, boolean isInternal, ShadowingContainer<Field> fields) {
    this.uniqueId = uniqueId;
    this.path = path;
    this.isInternal = isInternal;
    this.fields = fields;
  }

  public Optional<Field> getField(Name name) {
    return fields.getByName(name);
  }

  public RelDataType getRowType() {
    List<RelDataTypeField> fields = this.fields.stream()
        .filter(f->f instanceof Column)
        .map(f->(Column)f)
        .map(Column::getRelDataTypeField)
        .collect(Collectors.toList());

    return new SqrlCalciteTable(this, fields);
  }

  public Name getId() {
    return Name.system(getName() + ID_DELIMITER + uniqueId);
  }

  @Override
  public Name getName() {
    return path.getLast();
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
    Optional<Field> field = getField(namePath.getFirst());
    if (field.isEmpty()) {
      return Optional.empty();
    }

    if (namePath.getLength() == 1) {
      return field;
    }

    if (field.get() instanceof Relationship) {
      Relationship relationship = (Relationship) field.get();
      return relationship.getToTable()
          .walkField(namePath.popFirst());
    }

    return Optional.of(field.get());
  }

  public Optional<Table> walk(NamePath namePath) {
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
          .walk(namePath.popFirst());
    }

    return Optional.empty();
  }

  public Optional<Table> getParent() {
    for (Field field : fields) {
      if (field instanceof Relationship && ((Relationship) field).getJoinType() == JoinType.PARENT) {
        return Optional.of(((Relationship) field).getToTable());
      }
    }
    return Optional.empty();
  }

  @Override
  public String toString() {
    return "Table{" +
        "name=" + getName() +
        '}';
  }

  /**
   * Determines the next field name
   */
  public Name getNextFieldId(Name name) {
    return Name.system(name.getCanonical() + ID_DELIMITER + getNextFieldVersion(name));
  }

  public int getNextFieldVersion(Name name) {
    int version = 0;
    if (getField(name).isPresent()) {
      version = getField(name).get().getVersion() + 1;
    }
    return version;
  }

  //Todo: Validate first
  public Optional<List<Field>> walkFields(NamePath names) {
    List<Field> fields = new ArrayList<>();
    Table current = this;
    Name[] namesNames = names.getNames();
    for (Name field : namesNames) {
      Optional<Field> f = current.getField(field);
      if (f.isEmpty()) {
        return Optional.empty();
      }
      if (f.get() instanceof Relationship) {
        Relationship rel = (Relationship) current.getField(field).get();
        current = rel.getToTable();
      } else {
        current = null;
      }
      fields.add(f.get());
    }
    if (fields.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(fields);
  }

  public List<Column> getParentPrimaryKeys() {
    return this.fields.stream()
        .filter(f->f instanceof Column && ((Column)f).containsAttribute(ForeignKey.class))
        .map(f->(Column) f)
        .collect(Collectors.toList());
  }

  public List<Column> getPrimaryKeys() {
    return this.fields.stream()
        .filter(f->f instanceof Column && ((Column) f).isPrimaryKey())
        .map(f->(Column) f)
        .collect(Collectors.toList());
  }

  public List<Column> getColumns() {
    return this.fields.getElements().stream()
        .filter(f->f instanceof Column && !((Column) f).isInternal)
        .map(f->(Column) f)
        .collect(Collectors.toList());
  }

  public List<Relationship> getRelationships() {
    return this.fields.getElements().stream()
        .filter(f->f instanceof Relationship)
        .map(f->(Relationship) f)
        .collect(Collectors.toList());
  }
}
