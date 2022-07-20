package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.Relationship.JoinType;
import lombok.Getter;
import lombok.NonNull;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
public class Table {

  @NonNull
  final NamePath path;
  @NonNull
  final FieldList fields = new FieldList();

  public Table(@NonNull NamePath path) {
    this.path = path;
  }

  public Name getName() {
    return path.getLast();
  }

  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    s.append("Table[path=").append(path).append("]{\n");
    for (Field f : fields.getAccessibleFields()) s.append("\t").append(f).append("\n");
    s.append("}");
    return s.toString();
  }

  private int getNextFieldVersion(Name name) {
    return fields.nextVersion(name);
  }

  public Column addColumn(Name name, boolean visible) {
    Column col = new Column(name, getNextFieldVersion(name), visible);
    fields.addField(col);
    return col;
  }

  public Relationship addRelationship(Name name, Table toTable, Relationship.JoinType joinType,
                                                               Relationship.Multiplicity multiplicity) {
    Relationship rel = new Relationship(name, getNextFieldVersion(name), this, toTable, joinType, multiplicity);
    fields.addField(rel);
    return rel;
  }

  public Optional<Field> getField(Name name) {
    return getField(name,false);
  }

  public Optional<Field> getField(Name name, boolean fullColumn) {
    return fields.getAccessibleField(name);
  }

  public Optional<Table> walkTable(NamePath namePath) {
    if (namePath.isEmpty()) {
      return Optional.of(this);
    }
    Optional<Field> field = getField(namePath.getFirst());
    if (field.isEmpty() || !(field.get() instanceof Relationship)) {
      return Optional.empty();
    }
    Relationship rel = (Relationship) field.get();
    Table target = rel.getToTable();
    return target.walkTable(namePath.popFirst());
  }

  public Stream<Relationship> getAllRelationships() {
    return fields.getFields(true).filter(Relationship.class::isInstance).map(Relationship.class::cast);
  }

  public int getNextColumnIndex() {
    return fields.numFields();
  }

//  public List<Column> getPrimaryKeys() {
//    return fields.stream()
//            .filter(c->c instanceof Column)
//            .map(c->(Column)c)
//            .filter(c-> c.isPrimaryKey()).collect(Collectors.toList());
//  }


  public Optional<Table> getParent() {
    return getAllRelationships().filter(r -> r.getJoinType() == JoinType.PARENT).map(Relationship::getToTable).findFirst();
  }

  public Collection<Table> getChildren() {
    return getAllRelationships().filter(r -> r.getJoinType() == JoinType.CHILD).map(Relationship::getToTable).collect(Collectors.toList());
  }

  public List<Column> getVisibleColumns() {
    return fields.getFields(true).filter(Column.class::isInstance).map(Column.class::cast).collect(Collectors.toList());
  }

  public NamePath getPath() {
    return path; //TODO: is this what this method is supposed to return??
//    if (getField(ReservedName.PARENT).isPresent()) {
//      return ((Relationship) getField(ReservedName.PARENT).get())
//          .getToTable().getPath().concat(getName());
//    } else {
//      return getName().toNamePath();
//    }
  }


}
