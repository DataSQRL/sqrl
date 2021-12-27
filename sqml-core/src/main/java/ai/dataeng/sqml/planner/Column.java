package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.type.basic.BasicType;
import ai.dataeng.sqml.type.constraint.Constraint;
import ai.dataeng.sqml.type.constraint.ConstraintHelper;
import java.util.List;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString(callSuper = true)
public class Column extends Field {

  //Identity of the column in addition to name
  public final int version;

  //Type definition
  public final BasicType type;
  public final int arrayDepth;
  public final boolean nonNull;
  public final List<Constraint> constraints;

  //System information
  public final boolean isPrimaryKey;
  public final boolean isInternal;

  public Column(Name name, Table table, int version,
      BasicType type, int arrayDepth, List<Constraint> constraints,
      boolean isPrimaryKey, boolean isInternal) {
    super(name, table);
    if (name.getCanonical().startsWith("category")) {
      System.out.println();
    }
    this.version = version;
    this.type = type;
    this.arrayDepth = arrayDepth;
    this.constraints = constraints;
    this.isPrimaryKey = isPrimaryKey;
    this.isInternal = isInternal;
    this.nonNull = ConstraintHelper.isNonNull(constraints);
  }

  public String getId() {
    return name.getCanonical() + LogicalPlanImpl.ID_DELIMITER + Integer.toHexString(version);
  }

  @Override
  public boolean isVisible() {
    return !isInternal;
  }

  @Override
  public Field copy() {
    return null;
  }
}
