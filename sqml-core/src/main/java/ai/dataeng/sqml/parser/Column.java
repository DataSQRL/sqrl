package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.VersionedName;
import ai.dataeng.sqml.type.basic.BasicType;
import ai.dataeng.sqml.type.constraint.Constraint;
import ai.dataeng.sqml.type.constraint.ConstraintHelper;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@ToString(callSuper = true)
public class Column extends Field {

  private final Table table;
  //Identity of the column in addition to name
  @Setter
  public int version;

  //Type definition
  @Setter
  public BasicType type;
  public final int arrayDepth;
  public final boolean nonNull;
  public final List<Constraint> constraints;

  //System information
  @Setter
  public boolean isPrimaryKey;
  @Setter
  public boolean isForeignKey;
  @Setter
  public Optional<Column> fkReferences;
  public final boolean isInternal;

  public Column(Name name, Table table, int version,
      BasicType type, int arrayDepth, List<Constraint> constraints,
      boolean isPrimaryKey, boolean isForeignKey, Optional<Column> fkReferences,
      boolean isInternal) {
    super(name);
    this.table = table;
    this.version = version;
    this.type = type;
    this.arrayDepth = arrayDepth;
    this.constraints = constraints;
    this.isPrimaryKey = isPrimaryKey;
    this.isForeignKey = isForeignKey;
    this.fkReferences = fkReferences;
    this.isInternal = isInternal;
    this.nonNull = ConstraintHelper.isNonNull(constraints);
  }

  public static Column createTemp(String name, BasicType type, Table table, int version) {
    return new Column(Name.system(name),
        table, version, type, 0, List.of(), false, false, null, false
          );
  }

  public VersionedName getId() {
    return VersionedName.of(name, version);
  }

  @Override
  public boolean isVisible() {
    return !isInternal;
  }
}
