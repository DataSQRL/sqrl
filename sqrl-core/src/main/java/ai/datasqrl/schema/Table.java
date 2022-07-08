package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.local.analyzer.Analysis.TableVersion;
import ai.datasqrl.schema.Relationship.JoinType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import com.google.common.base.Preconditions;

@Getter
public class Table extends AbstractTable {
  TableVersion currentVersion = new TableVersion(this, 0);

  public void addExpressionColumn(Column column) {
    fields.add(column);
    currentVersion = new TableVersion(this, currentVersion.getVersion() + 1);
  }

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
    Preconditions.checkNotNull(fields.contains(timestamp));
  }

  @Override
  public String toString() {
    String s = super.toString();
    s += "[" + type + "," + timestamp + "," + statistic + "]";
    return s;
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

    return field;
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

  /**
   * Determines the next field name
   */
  public Name getNextFieldId(Name name) {
    int nextVersion = getNextColumnVersion(name);
    if (nextVersion != 0) {
      return name.suffix(Integer.toString(nextVersion));
    } else {
      return name;
    }
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
    return getAllColumns().filter(Column::isParentPrimaryKey).collect(Collectors.toList());
  }

  public List<Column> getVisibleColumns() {
    return getAllColumns().filter(Column::isVisible).collect(Collectors.toList());
  }

  public NamePath getPath() {
    if (getField(ReservedName.PARENT).isPresent()) {
      return ((Relationship) getField(ReservedName.PARENT).get())
          .getToTable().getPath().concat(getName());
    } else {
      return getName().toNamePath();
    }
  }

  public RootTableField asField() {
    return new RootTableField(this);
  }
}
