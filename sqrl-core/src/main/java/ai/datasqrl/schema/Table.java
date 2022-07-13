package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.schema.Relationship.JoinType;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class Table extends AbstractTable {

  public enum Type {
    STREAM, //a stream of records with synthetic (i.e. uuid) primary key ordered by timestamp
    STATE; //table with natural primary key that ensures uniqueness and timestamp for change-stream
  }

  @NonNull private final Type type;
  @NonNull private TableTimestamp timestamp;
  @NonNull private final TableStatistic statistic;

  public Table(int uniqueId, NamePath path, Type type, ShadowingContainer<Field> fields,
               TableTimestamp timestamp, TableStatistic statistic) {
    super(uniqueId,path,fields);
    this.type = type;
    this.timestamp = timestamp;
    this.statistic = statistic;
//    Preconditions.checkNotNull(fields.contains(timestamp));
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

  @Override
  public String toString() {
    String s = super.toString();
    s += "[" + type + "," + timestamp + "," + statistic + "]";
    return s;
  }

}
