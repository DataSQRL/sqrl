package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.VersionedName;
import ai.datasqrl.schema.type.basic.BasicType;
import ai.datasqrl.schema.type.constraint.Constraint;
import ai.datasqrl.schema.type.constraint.ConstraintHelper;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Column extends Field {
  private final Table table;
  //Identity of the column in addition to name
  public int version;

  //Type definition
  public final int arrayDepth;
  public final boolean nonNull;
  public final List<Constraint> constraints;

  //System information
  public boolean isPrimaryKey;
  public boolean isForeignKey;
  public boolean isInternal;
  //Equivalence testing: Todo: move out
  private Field source;
  private boolean parentPrimaryKey;

  public Column(Name name, Table table, int version,
      int arrayDepth, List<Constraint> constraints,
      boolean isPrimaryKey, boolean isForeignKey,
      boolean isInternal) {
    super(unboxName(name));
    this.table = table;
    this.version = version;
    this.arrayDepth = arrayDepth;
    this.constraints = constraints;
    this.isPrimaryKey = isPrimaryKey;
    this.isForeignKey = isForeignKey;
    this.isInternal = isInternal;
    this.nonNull = ConstraintHelper.isNonNull(constraints);
  }

  private static Name unboxName(Name name) {
    if (name instanceof VersionedName) {
      return ((VersionedName)name).toName();
    } else {
      return name;
    }
  }

  public static Column createTemp(Name name, BasicType type, Table table, int version) {
    return new Column(name,
        table, version, 0, List.of(), false, false, false
          );
  }

  public VersionedName getId() {
    return VersionedName.of(name, version);
  }

  @Override
  public boolean isVisible() {
    return !isInternal;
  }

  public void setSource(Field source) {
    this.source = source;
  }

  public Field getSource() {
    if (source == null) {
      return this;
    }
    Column s = (Column) this.source;
    return s.getSource();
  }

  public Column copy() {
    return new Column(this.name, this.table, this.version, this.arrayDepth, this.constraints,
        this.isPrimaryKey, this.isForeignKey, this.isInternal);
  }
}
